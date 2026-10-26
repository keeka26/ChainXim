# 字面量与有限数校验供原生配置入口使用。
import ast
import math
import configparser
import logging
import time
import random  # 仅初始化原有 RNG

import numpy as np

import global_var
from environment import Environment



def get_time(f):
    def inner(*arg, **kwarg):
        s_time = time.time()
        res = f(*arg, **kwarg)
        e_time = time.time()
        print('耗时：{}秒'.format(e_time - s_time))
        return res
    return inner

@get_time
def run(Z:Environment, total_round: int, max_height: int,
        process_bar_type, post_verification:bool, quantile: float):
    Z.exec(total_round, max_height, process_bar_type, post_verification)
    return Z.view_and_write(quantile)

def config_log(env_config:dict):
    """配置日志"""
    level_str = env_config.get('log_level', 'info')
    if level_str == 'error':
        global_var.set_log_level(logging.ERROR)
    elif level_str == 'warning':
        global_var.set_log_level(logging.WARNING)
    elif level_str == 'info':
        global_var.set_log_level(logging.INFO)
    elif level_str == 'debug':
        global_var.set_log_level(logging.DEBUG)
    else:
        raise ValueError("config error log_level must be set as " +
                         "error/warning/info/debug, cur setting:%s", level_str)
    logging.basicConfig(filename=global_var.get_result_path() / 'events.log',
                        level=global_var.get_log_level(), filemode='w')


# 固定首版协议配置；算法和 CLI 新开关留后续阶段。
DAG_DEFAULTS = {
    'protocol_version': 'conflux_2018_v4',
    'header_version': 2,
    'parent_rule': 'pivot_and_all_tips',
    'confirmation_policy': 'none',
    'drain_rounds': 0,
    'seed': 7,
    'size_model': 'fixed_total',
    'sync_interval': 5,  # 定期全frontier公告轮间隔
    'sync_batch_size': 128,  # 每次库存交换最多排入的块数
    'plot_miner_id': 0,  # 仅绘图，不改变统计参考节点
}
TYPED_POW_TYPES = ('consensus.PoW', 'consensus.VirtualPoW',
                   'consensus.SolidPoW', 'consensus.PoS', 'consensus.ConfluxPoW')


def _setting(config, args, section, key, default):
    """只把 None 当作缺值，保留显式的 0、False 和空列表。"""
    if args.get(key) is not None:
        return args[key]
    return config.get(section, key, fallback=default)


def _integer(value, key, minimum=0):
    """严格解析整数；不截断小数、不接受 bool 作为节点数。"""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f'{key} must be an integer >= {minimum}')
    try:
        result = int(value)
    except ValueError as error:
        raise ValueError(f'{key} must be an integer >= {minimum}') from error
    if result < minimum:
        raise ValueError(f'{key} must be an integer >= {minimum}')
    return result


def _nonnegative_number(value, key):
    """数值进入预算/难度前检查有限性及非负范围。"""
    if isinstance(value, bool):
        raise ValueError(f'{key} must be finite and nonnegative')
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError) as error:
        raise ValueError(f'{key} must be finite and nonnegative') from error
    if not math.isfinite(result) or result < 0:
        raise ValueError(f'{key} must be finite and nonnegative')
    return result


def _boolean(value, key):
    """接受 ConfigParser 的布尔字面值或调用者的 bool。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in configparser.ConfigParser.BOOLEAN_STATES:
        return configparser.ConfigParser.BOOLEAN_STATES[value.lower()]
    raise ValueError(f'{key} must be a boolean')


def _literal(value, key):
    """仅解析列表/元组/字典等 Python 字面量，不执行配置表达式。"""
    if not isinstance(value, str):
        return value
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError) as error:
        raise ValueError(f'{key} must be a Python literal, not an expression') from error


def _read_attack_arg(config, args) -> dict:
    """先验证 INI，再合并 CLI 字段，空覆盖不屏蔽原字典。"""
    configured = _literal(config.get('AttackSettings', 'attack_arg', fallback='{}'), 'attack_arg')
    overrides = _literal(args.get('attack_arg'), 'attack_arg') if args.get('attack_arg') is not None else {}
    if not isinstance(configured, dict) or not isinstance(overrides, dict):
        raise ValueError('attack_arg must be a dictionary literal in both INI and overrides')
    merged = dict(configured)
    merged.update(overrides)
    return merged


def read_pow_settings(config: configparser.ConfigParser, args: dict,
                      miner_num: int, consensus_type: str) -> dict:
    """原生 PoW 参数入口，也适用于ConfluxPoW。

    返回 target:str(64 hex)、q_ave:float、q_distr:'equal'|list[int | float]、N:int。
    不创建节点、不修改全局变量；target 到 bytes 的转换由共识类负责。
    旧 PoW rand 保留；Conflux 首版只接受 equal/数值列表。
    """
    miner_num = _integer(miner_num, 'miner_num', 1)
    q_ave = _nonnegative_number(_setting(config, args, 'ConsensusSettings', 'q_ave', 1), 'q_ave')
    q_distr = _setting(config, args, 'ConsensusSettings', 'q_distr', 'equal')
    if isinstance(q_distr, str) and q_distr == 'rand':
        if consensus_type == 'consensus.ConfluxPoW':
            raise ValueError('q_distr=rand is not supported by the initial Conflux profile; use equal or a list')
        q_distr = get_random_q_gaussian(miner_num, q_ave)
    elif isinstance(q_distr, str) and q_distr != 'equal':
        q_distr = _literal(q_distr, 'q_distr')
    if not (isinstance(q_distr, str) and q_distr == 'equal'):
        if not isinstance(q_distr, list) or len(q_distr) != miner_num:
            raise ValueError('q_distr must be equal or a numeric list with length miner_num')
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in q_distr):
            raise ValueError('q_distr list elements must be numbers, not strings or bool')
        # 验证后保留 int/float 身份，维持旧 parameters 文本。
        for value in q_distr:
            _nonnegative_number(value, 'q_distr')
        q_distr = list(q_distr)
    average_block_time = _nonnegative_number(
        _setting(config, args, 'ConsensusSettings', 'average_block_time', 0), 'average_block_time')
    target = _setting(config, args, 'ConsensusSettings', 'target', 'F' * 64)
    if not isinstance(target, str) or len(target) != 64 or any(
            char not in '0123456789abcdefABCDEF' for char in target):
        raise ValueError('target must be exactly 64 hexadecimal characters')
    total_hash_budget = miner_num * q_ave if q_distr == 'equal' else sum(q_distr)
    expected_trials = total_hash_budget * average_block_time
    if not math.isfinite(total_hash_budget) or not math.isfinite(expected_trials):
        raise ValueError('q_distr/q_ave and average_block_time produce a nonfinite total budget')
    # 零总预算语义保留；PoS 仍使用原配置目标。
    if average_block_time > 0 and total_hash_budget > 0 and consensus_type != 'consensus.PoS':
        denominator = int(expected_trials)
        if denominator == 0:
            raise ValueError('average_block_time * total hash budget must be >= 1; use average_block_time=0 with an explicit target')
        # 目标边界修正：除数 1 时旧公式产生 65 位目标，封顶于合法 256 bit 最大值。
        target = f'{min(2**256 - 1, 2**256 // denominator):064x}'
    params = {'target': target, 'q_ave': q_ave, 'q_distr': q_distr,
              'N': _integer(_setting(config, args, 'ConsensusSettings', 'N', 6), 'N')}
    if consensus_type == 'consensus.PoS' and config.has_option('ConsensusSettings', 'precompute_threshold'):
        params['precompute_threshold'] = _integer(config.get('ConsensusSettings', 'precompute_threshold'),
                                                'precompute_threshold')
    return params


# 数据项保留原section/变量名，合法0不被or覆盖。
def read_dataitem_settings(config, args):
    """DAG启动前验证8字节抽象项的容量、名义MB大小和输入间隔。"""
    return {'dataitem_enable': _boolean(_setting(config, args, 'DataItemSettings', 'dataitem_enable', False), 'dataitem_enable'),
            **{key: _integer(_setting(config, args, 'DataItemSettings', key, default), key)
               for key, default in (('max_block_capacity', 0), ('dataitem_size', 1), ('dataitem_input_interval', 0))}}


def read_dag_settings(config: configparser.ConfigParser, args: dict) -> dict:
    """返回纯 DAG 配置，并在启动前检查首版的有效组合。

    本接口仅为选择 Conflux 的入口调用；旧单链不读取/要求 DAGSettings。
    验证不依赖 Results/global chain；未知键报错，未来参数保留但启用时报错。
    """
    if config.has_section('DAGSettings'):
        unknown = set(config['DAGSettings']) - set(DAG_DEFAULTS)
        if unknown:
            raise ValueError(f'unknown DAGSettings key(s): {", ".join(sorted(unknown))}')
    values = {key: _setting(config, args, 'DAGSettings', key, default)
              for key, default in DAG_DEFAULTS.items()}
    for key in ('header_version', 'drain_rounds', 'seed'):
        values[key] = _integer(values[key], key)
    for key in ('protocol_version', 'header_version', 'parent_rule'):
        if values[key] != DAG_DEFAULTS[key]:
            raise ValueError(f'{key} must be {DAG_DEFAULTS[key]} for the initial Conflux profile')
    # 两种确认政策跨所有基础网络配置生效。
    if values['confirmation_policy'] not in ('none', 'pivot_depth'):
        raise ValueError('confirmation_policy must be none or pivot_depth')
    # 同步参数和两个明确计费模型。
    for key in ('sync_interval', 'sync_batch_size'):
        values[key] = _integer(values[key], key, 1)
    if values['size_model'] not in ('fixed_total', 'body_plus_header'):
        raise ValueError('size_model must be fixed_total or body_plus_header')
    # NumPy legacy seed 接受 uint32；0 保留。
    if values['seed'] >= 2**32:
        raise ValueError('seed must be an integer in [0, 2**32-1]')
    network_type = _setting(config, args, 'EnvironmentSettings', 'network_type', 'network.LockstepNetwork')
    
    if network_type not in tuple('network.' + name for name in ('LockstepNetwork', 'DeterPropNetwork', 'StochPropNetwork', 'TopologyNetwork', 'AdHocNetwork')):
        raise ValueError('Conflux supports Lockstep, DeterProp, StochProp, Topology and AdHoc networks')
    _validate_conflux_runtime(config, args, network_type)
    miner_num = _integer(_setting(config, args, 'EnvironmentSettings', 'miner_num', 5), 'miner_num', 1)
    # CLI/INI覆盖保留0；非法节点在创建Results前拒绝。
    values['plot_miner_id'] = _integer(values['plot_miner_id'], 'plot_miner_id')
    if values['plot_miner_id'] >= miner_num:
        raise ValueError('plot_miner_id must be in [0, miner_num-1]')
    for key, default in (('total_round', 200), ('total_height', 1000000)):
        _integer(_setting(config, args, 'EnvironmentSettings', key, default), key)
    _nonnegative_number(_setting(config, args, 'EnvironmentSettings', 'blocksize', 0.1), 'blocksize')
    # 数据项能力启用；原公共前缀判据仍不能套用DAG。
    read_dataitem_settings(config, args)
    for section, key in (('EnvironmentSettings', 'common_prefix_enable'),):
        if _boolean(_setting(config, args, section, key, False), key):
            raise ValueError(f'{key}=True is not implemented for Conflux')
    adver_num = _integer(_setting(config, args, 'AttackSettings', 'adver_num', 0), 'adver_num')
    adversary_ids = _literal(_setting(config, args, 'AttackSettings', 'adver_lists', 'None'), 'adver_lists')
    if adversary_ids is not None and not isinstance(adversary_ids, (list, tuple)):
        raise ValueError('adver_lists must be a list/tuple or None')
    if adver_num > 0 or adversary_ids:
        raise ValueError('adver_num/adver_lists: active attacks are not implemented for Conflux')
    # 与实际原生入口同样验证/合并两侧字典。
    _read_attack_arg(config, args)
    return values



# 在创建 Results 或节点前核查输出/有效网络参数。
def _validate_conflux_runtime(config, args, network_type):
    """只验证原模型实际消费的输入，不替换传播算法或引入默认回退。"""
    if _setting(config, args, 'EnvironmentSettings', 'process_bar_type', 'round') not in ('round', 'height'):
        raise ValueError('process_bar_type must be round or height')
    if _setting(config, args, 'EnvironmentSettings', 'log_level', 'warning') not in ('error', 'warning', 'info', 'debug'):
        raise ValueError('log_level must be error/warning/info/debug')
    for key in ('show_fig', 'compact_outputfile'):
        _boolean(_setting(config, args, 'EnvironmentSettings', key, False), key)
    quantile = float(config.get('EnvironmentSettings', 'consensus_miner_quantile', fallback='-1'))
    if not math.isfinite(quantile) or not (quantile == -1 or 0 <= quantile <= 1):
        raise ValueError('consensus_miner_quantile must be -1 or in [0, 1] (retained inactive for DAG)')
    if network_type == 'network.DeterPropNetwork':
        vector = _literal(_setting(config, args, 'DeterPropNetworkSettings', 'prop_vector', None), 'prop_vector')
        if (not isinstance(vector, list) or not vector
                or any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x)
                       or not 0 <= x <= 1 for x in vector)
                or vector[-1] != 1 or any(a > b for a, b in zip(vector, vector[1:]))):
            raise ValueError('prop_vector must be a nonempty nondecreasing numeric list in [0, 1], ending in 1')
    elif network_type == 'network.StochPropNetwork':
        for key in ('rcvprob_start', 'rcvprob_inc'):
            value = _nonnegative_number(_setting(config, args, 'StochPropNetworkSettings', key, None), key)
            if value > 1:
                raise ValueError(f'{key} must be in [0, 1]')
        rates = _literal(_setting(config, args, 'StochPropNetworkSettings', 'stat_prop_times', None), 'stat_prop_times')
        if (not isinstance(rates, list) or any(isinstance(x, bool) or not isinstance(x, (int, float))
                or not math.isfinite(x) or not 0 < x <= 1 for x in rates) or len(set(rates)) != len(rates)):
            raise ValueError('stat_prop_times must be a unique numeric list in (0, 1]')


    elif network_type in ('network.TopologyNetwork', 'network.AdHocNetwork'):
        # 必须有对应原网络节；缺失/非法参数不先创建Results。
        section = 'TopologyNetworkSettings' if network_type == 'network.TopologyNetwork' else 'AdHocNetworkSettings'
        if not config.has_section(section):
            raise ValueError(f'network section {section} is required')
        def number(key, positive=False, maximum=None):
            value = _nonnegative_number(_setting(config,args,section,key,None),key)
            if positive and value <= 0 or maximum is not None and value > maximum:
                raise ValueError(f'{key} is outside its supported range')
            return value
        count = _integer(_setting(config,args,'EnvironmentSettings','miner_num',5),'miner_num',1)
        number('ave_degree',maximum=count-1)
        number('outage_prob',maximum=1)
        rates = _literal(_setting(config,args,section,'stat_prop_times',None),'stat_prop_times')
        if not isinstance(rates,list) or any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) or not 0<x<=1 for x in rates) or len(set(rates)) != len(rates):
            raise ValueError('stat_prop_times must be a unique numeric list in (0, 1]')
        mode = _setting(config,args,section,'init_mode',None)
        if section == 'TopologyNetworkSettings':
            if mode not in ('rand','coo','adj'):
                raise ValueError('init_mode must be rand/coo/adj')
            if _setting(config,args,section,'rand_mode',None) not in ('binomial','homogeneous'):
                raise ValueError('rand_mode must be binomial/homogeneous')
            for key in ('bandwidth_honest','bandwidth_adv','avg_tp_change_interval'):
                number(key,positive=True)
            for key in ('edge_remove_prob','edge_add_prob'):
                number(key,maximum=1)
            _integer(_setting(config,args,section,'max_allowed_partitions',None),'max_allowed_partitions',1)
            for key in ('show_label','save_routing_graph','save_routing_history','dynamic','enable_resume_transfer'):
                _boolean(_setting(config,args,section,key,None),key)
            if mode in ('adj','coo'):
                _validate_dag_topology_file(mode,_setting(config,args,section,'topology_path',None),count)
        else:
            if mode != 'rand':
                raise ValueError('AdHoc init_mode must be rand')
            for key in ('region_width','comm_range','segment_size','bandwidth_max'):
                number(key,positive=True)
            number('move_variance')
            _boolean(_setting(config,args,section,'enable_large_scale_fading',None),'enable_large_scale_fading')
            if _setting(config,args,section,'path_loss_level',None) not in ('low','medium','high'):
                raise ValueError('path_loss_level must be low/medium/high')


def _validate_dag_topology_file(mode, filename, miner_num):
    """原adj/COO CSV在创建Results前做数值/形状验证。

    仍交给原Topology生成器建图；此处不建立第二个网络，COO带宽为MB/round。
    """
    import csv
    filename = filename or ('conf/topologies/default_adj.csv' if mode == 'adj'
                            else 'conf/topologies/circular16_coo.csv')
    try:
        with open(filename, newline='', encoding='utf-8') as f:
            rows = [[float(x) for x in row] for row in csv.reader(f)]
    except (OSError, ValueError) as e:
        raise ValueError('topology CSV must exist and contain numeric values') from e
    if not rows or any(not row for row in rows) or any(not math.isfinite(x) for row in rows for x in row):
        raise ValueError('topology CSV must contain finite nonempty rows')
    if mode == 'adj':
        if len(rows) != miner_num or any(len(row) != miner_num for row in rows):
            raise ValueError('topology adjacency shape must match miner_num')
        if any(rows[i][i] != 0 or any(rows[i][j] != rows[j][i]
               for j in range(miner_num)) for i in range(miner_num)):
            raise ValueError('topology adjacency must be symmetric with zero diagonal')
    else:
        if len(rows) != 3 or len({len(row) for row in rows}) != 1:
            raise ValueError('topology COO must have three equal-width rows')
        for src, target, bandwidth in zip(*rows):
            if (not src.is_integer() or not target.is_integer() or not 0 <= src < miner_num
                    or not 0 <= target < miner_num or src == target or bandwidth <= 0):
                raise ValueError('topology COO endpoints/bandwidth are invalid')


def initialize_seed(seed: int):
    """只设置 random/NumPy；SHA/template 不加入随机字段。"""
    random.seed(seed)
    np.random.seed(seed)


def main(**args):
    '''主程序'''
    # 读取配置文件
    config = configparser.ConfigParser()
    config.optionxform = lambda option: option
    config.read(args.get('config'),encoding='utf-8')
    env_config = dict(config['EnvironmentSettings'])
    #设置全局变量
    # 选择类型在创建结果目录之前完成。
    consensus_type = _setting(config, args, 'EnvironmentSettings', 'consensus_type', 'consensus.PoW')
    miner_num = _integer(_setting(config, args, 'EnvironmentSettings', 'miner_num', 5), 'miner_num', 1)
    network_type = _setting(config, args, 'EnvironmentSettings', 'network_type', 'network.LockstepNetwork')
    
    # DAG配置在创建结果目录之前完成验证。
    dag_settings = read_dag_settings(config, args) if consensus_type == 'consensus.ConfluxPoW' else {}
    if dag_settings:
        read_pow_settings(config, args, miner_num, consensus_type)
        initialize_seed(dag_settings['seed'])
    global_var.__init__(args.get('result_path'))
    # 使用上述有效共识选择，旧路径保持原类型名。
    global_var.set_dag_settings(dag_settings)  # 
    global_var.set_consensus_type(consensus_type)
    global_var.set_network_type(network_type)
    global_var.set_miner_num(miner_num)
    # 0MB 是有效覆盖值，只在 CLI 未提供时读取 INI。
    global_var.set_blocksize(
        args['blocksize'] if args.get('blocksize') is not None
        else float(env_config['blocksize']))
    # 布尔覆盖也区分 False 与未提供。
    global_var.set_show_fig(_boolean(_setting(config, args, 'EnvironmentSettings', 'show_fig', False), 'show_fig'))
    global_var.set_compact_outputfile(
        config.getboolean('EnvironmentSettings','compact_outputfile')
        if not args.get('no_compact_outputfile') else False)
    # 保留旧 INI，支持显式 False 调用覆盖。
    global_var.set_common_prefix_enable(_boolean(_setting(config, args, 'EnvironmentSettings', 'common_prefix_enable', False), 'common_prefix_enable'))
    
    # 配置日志
    config_log(env_config)
    
    # 设置PoW共识协议参数
    consensus_settings = dict(config['ConsensusSettings'])
    # Conflux 与原 PoW 使用同一 typed 参数入口。
    if consensus_type in TYPED_POW_TYPES:
        consensus_param = read_pow_settings(config, args, miner_num, consensus_type)
        # DAG配置通过原consensus_param传给每个实例。
        if dag_settings:
            consensus_param['dag_settings'] = dict(dag_settings)
        global_var.set_ave_q(consensus_param['q_ave'])
        global_var.set_PoW_target(consensus_param['target'])
    else:
        consensus_param = {}
        for key, value in consensus_settings.items():
            consensus_param[key] = args.get(key) or value
        if 'N' in consensus_param:
            consensus_param['N'] = int(consensus_param['N'])

    # 设置网络参数
    network_param = {}
    # StochPropNetwork
    if network_type == 'network.StochPropNetwork':
        bdnet_settings = dict(config["StochPropNetworkSettings"])
        network_param = {
            'rcvprob_start': (args.get('rcvprob_start') 
                              if args.get('rcvprob_start') is not None  
                              else float(bdnet_settings['rcvprob_start'])),
            'rcvprob_inc': (args.get('rcvprob_inc') 
                            if args.get('rcvprob_inc') is not None
                            else float(bdnet_settings['rcvprob_inc'])),
            'stat_prop_times': (args.get('stat_prop_times') or 
                                _literal(bdnet_settings['stat_prop_times'], 'stat_prop_times'))  # 安全字面量
        }
    # DeterPropNetwork
    elif network_type == 'network.DeterPropNetwork':
        pvnet_settings = dict(config["DeterPropNetworkSettings"])
        network_param = {'prop_vector':(args.get('prop_vector') or 
                                        _literal(pvnet_settings['prop_vector'], 'prop_vector'))}  # 安全字面量
    # TopologyNetwork
    elif network_type == 'network.TopologyNetwork':
        net_setting = 'TopologyNetworkSettings'
        bool_params  = ['show_label', 'save_routing_graph', 'save_routing_history',
                        'dynamic','enable_resume_transfer']
        float_params = ['ave_degree', 'bandwidth_honest', 'bandwidth_adv',
                        'outage_prob','avg_tp_change_interval','edge_add_prob',
                        'edge_remove_prob','max_allowed_partitions']
        for bparam in bool_params:
            network_param.update({bparam: args.get(bparam) or 
                                 config.getboolean(net_setting, bparam)})
        for fparam in float_params:
            # DAG扩展兼容修复：单链也保留显式0，仅None才回退到INI。
            cli_value = args.get(fparam)
            network_param[fparam] = (
                cli_value if cli_value is not None else config.getfloat(net_setting, fparam)
            )
        network_param.update({
            'init_mode': (args.get('init_mode') or 
                          config.get(net_setting, 'init_mode')),
            'topology_path': (args.get('topology_path') or 
                              config.get(net_setting, 'topology_path', fallback=None)),
            'stat_prop_times': (args.get('stat_prop_times') or 
                                _literal(config.get(net_setting, 'stat_prop_times'), 'stat_prop_times')),  # 安全字面量
            'rand_mode': (args.get('rand_mode') or
                          config.get(net_setting, 'rand_mode'))
        })
    # AdHocNetwork
    elif network_type == 'network.AdHocNetwork':
        net_setting = 'AdHocNetworkSettings'
        bool_params  = ['enable_large_scale_fading']
        float_params = ['ave_degree', 'region_width', 'comm_range','move_variance',
                        'outage_prob','segment_size','bandwidth_max'] # 'min_move', 'max_move'
        for bparam in bool_params:
            network_param.update({bparam: args.get(bparam) or 
                                 config.getboolean(net_setting, bparam)})
        for fparam in float_params:
            # DAG扩展兼容修复：与Topology一致，避免0被or替换成INI值。
            cli_value = args.get(fparam)
            network_param[fparam] = (
                cli_value if cli_value is not None else config.getfloat(net_setting, fparam)
            )
        network_param.update({
            'init_mode': (args.get('init_mode') or config.get(net_setting, 'init_mode')),
            'stat_prop_times': (args.get('stat_prop_times') or _literal(config.get(net_setting, 'stat_prop_times'), 'stat_prop_times')),  # 安全字面量
            'path_loss_level': (args.get('path_loss_level') or config.get(net_setting, 'path_loss_level')),
        })
        # global_var.set_segmentsize(config.getfloat(net_setting, "segment_size"))

    # 经过前置验证的显式值，包括空列表/0，进入原网络。
    if dag_settings and network_type == 'network.DeterPropNetwork':
        network_param['prop_vector'] = _literal(_setting(config, args, 'DeterPropNetworkSettings', 'prop_vector', None), 'prop_vector')
    if dag_settings and network_type == 'network.StochPropNetwork':
        network_param['stat_prop_times'] = _literal(_setting(config, args, 'StochPropNetworkSettings', 'stat_prop_times', None), 'stat_prop_times')

    # 沿原network_param通道，False/0不用or覆盖；旧路径保留。
    if dag_settings and network_type in ('network.TopologyNetwork','network.AdHocNetwork'):
        section = 'TopologyNetworkSettings' if network_type == 'network.TopologyNetwork' else 'AdHocNetworkSettings'
        for key in tuple(network_param):
            raw = _setting(config,args,section,key,None)
            if key == 'stat_prop_times':
                network_param[key] = _literal(raw,key)
            elif key in bool_params:
                network_param[key] = _boolean(raw,key)
            elif key in float_params:
                network_param[key] = _nonnegative_number(raw,key)
            else:
                network_param[key] = raw
        # 路由图依赖真实历史；manifest记录实际启用值。
        if network_type == 'network.TopologyNetwork' and network_param['save_routing_graph']:
            network_param['save_routing_history'] = True

    # 设置attack参数
    attack_setting = dict(config['AttackSettings'])
    global_var.set_attack_execute_type(args.get('attack_type') or attack_setting.get('attack_type', 'HonestMining'))
    attack_param = {
        'adver_num'    : (args.get('adver_num') if args.get('adver_num') is not None 
                          else int(attack_setting['adver_num'])),
        'attack_type'  : (args.get('attack_type') if args.get('attack_type') is not None
                          else attack_setting.get('attack_type', 'HonestMining')),
        # 使用同一字典解析/合并，避免纯接口与 CLI 契约不同。
        'attack_arg'   : _read_attack_arg(config, args),
        'adversary_ids': (args.get('adver_lists') if args.get('adver_lists') is not None
                          else _literal(attack_setting.get('adver_lists') or 'None', 'adver_lists')),  # 安全字面量
    }
    attack_param['attack_arg'].update({'N': consensus_param.get('N', 1)})

    # 设置dataitem相关的配置
    dataitem_setting = dict(config['DataItemSettings'])
    dataitem_param = {
        # 未传开关时沿用 INI，显式 False 可关闭。
        'dataitem_enable': _boolean(_setting(config, args, 'DataItemSettings', 'dataitem_enable', False), 'dataitem_enable'),
        'max_block_capacity': (args.get('max_block_capacity') or 
                               int(dataitem_setting['max_block_capacity'])),
        'dataitem_size': (args.get('dataitem_size') or 
                          int(dataitem_setting['dataitem_size'])),
        'dataitem_input_interval': (args.get('dataitem_input_interval') or 
                                    int(dataitem_setting['dataitem_input_interval'])),
    }

    # DAG使用已验证的typed参数；legacy配置处理不改。
    if consensus_type == 'consensus.ConfluxPoW':
        dataitem_param = read_dataitem_settings(config, args)

    # 生成环境
    genesis_blockheadextra = {}
    genesis_blockextra = {}

    Z = Environment(attack_param, consensus_param, network_param,
                    genesis_blockheadextra, genesis_blockextra, dataitem_param)
    # 零轮/零高度上限不能被 CLI/INI 回退吞掉。
    # 高度停止条件仍由 Environment 使用原 current_height > max_height。
    total_round = (args['total_round'] if args.get('total_round') is not None
                   else int(env_config['total_round']))
    max_height = (args['total_height'] if args.get('total_height') is not None
                  else int(env_config.get('total_height') or 2**31 - 2))
    process_bar_type = (args.get('process_bar_type') 
                        or env_config.get('process_bar_type'))

    return run(Z, total_round, max_height, process_bar_type, args.get('disable_post_verification'),
               config.getfloat('EnvironmentSettings','consensus_miner_quantile'))

def get_random_q_gaussian(miner_num,q_ave):
    '''
    随机设置各个节点的hash rate,满足均值为q_ave的高斯分布
    且满足全网总算力为q_ave*miner_num
    '''
    # 生成均值为ave_q，方差为0.2*q_ave的高斯分布
    q_dist = np.random.normal(q_ave, 0.2*q_ave, miner_num)
    # 归一化到总和为total_q，并四舍五入为整数
    total_q = int(round(q_ave * miner_num))
    q_dist = total_q / np.sum(q_dist) * q_dist
    q_dist = np.round(q_dist).astype(int)
    # 修正，如果和不为total_q就把差值分摊在最小值或最大值上
    diff = int(total_q - np.sum(q_dist))
    if diff != 0:
        sign_diff = 1 if diff > 0 else -1
        for _ in range(abs(diff)):
            idx = np.argmin(q_dist) if sign_diff > 0 else np.argmax(q_dist)
            q_dist[idx] += sign_diff
    return q_dist.tolist()


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    program_description = 'ChainXim, a blockchain simulator developed by XinLab\
, simulates and assesses blockchain system with various consensus protocols \
under different network conditions. Security evaluation of blockchain systems \
could be performed with attackers designed in the simulator'
    parser = argparse.ArgumentParser(description=program_description)
    parser.add_argument('--config', '-c', help='The path to the config file.', type=str, default='system_config.ini')
    # EnvironmentSettings
    env_setting = parser.add_argument_group('EnvironmentSettings','Settings for Environment')
    env_setting.add_argument('--process_bar_type', help='Set the style of process bar: round/height',type=str)
    env_setting.add_argument('--total_round', help='Total rounds before simulation stops.', type=int)
    env_setting.add_argument('--total_height', help='Total block height generated before simulation stops.', type=int)
    env_setting.add_argument('--miner_num', help='The total miner number in the network.', type=int)
    env_setting.add_argument('--consensus_type',help='The consensus class imported during simulation',type=str)
    env_setting.add_argument('--network_type',help='The network class imported during simulation',type=str)
    env_setting.add_argument('--blocksize', help='The size of each block in MB. Only effective when dataitem_enable=False and network_type is TopologyNetwork or AdhocNetwork.',type=float)
    env_setting.add_argument('--show_fig', help='Show figures during simulation.',action='store_true', default=None)  # 缺值区分
    env_setting.add_argument('--no_compact_outputfile', action='store_true', default=None,  # 
                             help='Simplify log and result outputs to reduce disk space consumption. Use no_compact_outputfile to disable this feature.')
    # CLI 覆盖 INI；未提供时使用已冻结 DAGSettings。
    dag_setting = parser.add_argument_group('DAGSettings', 'Native Conflux runtime settings')
    # 绘图节点与指标/停止条件的reference_miner_id独立。
    dag_setting.add_argument('--plot_miner_id', type=int, help='Node ID for the additional local DAG graph; default 0, without changing the statistics reference node.')
    dag_setting.add_argument('--seed', type=int, help='Random/NumPy seed for the Conflux profile.')
    # N仍用原-N；CLI仅选择经验确认政策。
    dag_setting.add_argument('--confirmation_policy', choices=('none', 'pivot_depth'), help='Reversible empirical pivot depth confirmation using original -N; default none.')
    dag_setting.add_argument('--drain_rounds', type=int, help='Receive-only rounds after mining; default is 0.')
    # INI/CLI同一配置接口。
    dag_setting.add_argument('--sync_interval', type=int, help='Rounds between all-frontier inventory exchanges.')
    dag_setting.add_argument('--sync_batch_size', type=int, help='Maximum missing blocks queued per inventory exchange.')
    dag_setting.add_argument('--size_model', choices=('fixed_total','body_plus_header'), help='Nominal total size or nominal body plus actual v2 header overhead.')
    # ConsensusSettings
    consensus_setting = parser.add_argument_group('ConsensusSettings', 'Settings for Consensus Protocol')
    consensus_setting.add_argument('--q_ave', help='The average number of hash trials in a round.',type=float)
    consensus_setting.add_argument('--q_distr', help='distribution of hash rate across all miners.\
                        \'equal\': all miners have equal hash rate;\
                        \'rand\': q satisfies gaussion distribution.',type=str)
    consensus_setting.add_argument('-N', help='The number of block confirmations required for the transactoins in a block to be considered valid. Used to calculate attack success rate.', type=int)
    # Ensure only one of difficulty and average_block_time can be set
    group = consensus_setting.add_mutually_exclusive_group()
    group.add_argument('--difficulty', help='The number of zero prefix of valid block hash.\
                    A metric for Proof of Work difficulty.', type=int)
    group.add_argument('--average_block_time', help='The average time interval between two blocks.\
                    If average_block_time=0, then target is set as the difficulty.', type=float)
    # AttackModeSettings
    attack_setting = parser.add_argument_group('AttackModeSettings','Settings for Attack')
    attack_setting.add_argument('--adver_num',help='The total number of attackers. If adver_num is non-zero and adversary_ids not specified, then attackers are randomly selected.',type=int)
    attack_setting.add_argument('--adver_lists', help='Specify id of adversaries. If adversary_ids is set, `adver_num` will not take effect.', type=str)
    attack_setting.add_argument('--attack_type', help='The name of attack type defined in attack mode.',type=str)
    attack_setting.add_argument('-Ng', help='Parameter Ng for DoubleSpending', type=int)
    attack_setting.add_argument('--eclipse_target', help='The target miner id for eclipse attack.', type=str)
    # StochPropNetworkSettings
    stoch_setting = parser.add_argument_group('StochPropNetworkSettings','Settings for StochPropNetwork')
    stoch_setting.add_argument('--rcvprob_start', help='Initial receive probability when a block access network.',type=float)
    stoch_setting.add_argument('--rcvprob_inc',help='Increment of rreceive probability per round.', type=float)
    # CommonSettings for both TopologyNetwork and AdhocNetwork
    common_setting = parser.add_argument_group('CommonSettings', 'Common settings for both TopologyNetwork and AdhocNetwork')
    init_mode_help_text = ''' Options:coo/adj/rand.
    coo: load adjecent matrix in COOrdinate format.
    adj: load adjecent matrix.
    rand: randomly generate a network. AdhocNetwork only support this.'''
    common_setting.add_argument('--init_mode', help='Initialization mode for the network.' + init_mode_help_text, type=str)
    common_setting.add_argument('--topology_path', help='The relative path to the topology file in a csv format specified by init_mode.', type=str)
    common_setting.add_argument('--ave_degree', help='Set the average degree of the network.', type=float)
    common_setting.add_argument('--outage_prob', help='The outage probability of each link.', type=float)
    # TopologyNetworkSettings
    topology_setting = parser.add_argument_group('TopologyNetworkSettings', 'Settings for TopologyNetwork')
    topology_setting.add_argument('--show_label', help='Show edge labels on network and routing graph.', action='store_true', default=None)  # 未提供不覆盖INI
    topology_setting.add_argument('--save_routing_graph', help='Generate routing graph at the end of simulation or not.', action='store_true', default=None)  # 未提供不覆盖INI
    rand_mode_help_text = '''Options:homogeneous/binomial.
    homogeneous: try to keep the degree of each node the same.
    binomial: set up edges with probability ave_degree/(miner_num-1). '''
    topology_setting.add_argument('--rand_mode', help=rand_mode_help_text, type=str)
    topology_setting.add_argument('--bandwidth_honest', help='Set bandwidth between honest miners and between the honest and adversaries(MB/round)', type=float)
    topology_setting.add_argument('--bandwidth_adv', help='Set bandwidth between adversaries(MB/round)', type=float)
    topology_setting.add_argument('--dynamic', help='Whether the network topology can dynamically change.', action='store_true', default=None)  # 未提供不覆盖INI
    topology_setting.add_argument('--avg_tp_change_interval', help='The average interval of topology changing.', type=float)
    topology_setting.add_argument('--edge_remove_prob', help='The probability of each links being removed in each round.', type=float)
    topology_setting.add_argument('--edge_add_prob', help='The probability of each links being added in each round.', type=float)
    topology_setting.add_argument('--max_allowed_partitions', help='The maximum number of network partitions being allowed.', type=int)
    # AdhocNetworkSettings
    adhoc_setting = parser.add_argument_group('AdhocNetworkSettings', 'Settings for AdhocNetwork')
    adhoc_setting.add_argument('--region_width', help='Width of the region for the network.', type=float)
    adhoc_setting.add_argument('--comm_range', help='Communication range.', type=float)
    adhoc_setting.add_argument('--move_variance', help='Variance of the movement when position updates in Gaussian random walk.', type=float)
    adhoc_setting.add_argument('--enable_large_scale_fading', help='If large-scale fading is enabled, the segment size will be adjusted automatically according to the fading model.', action='store_true', default=None)  # 未提供不覆盖INI
    adhoc_setting.add_argument('--path_loss_level', help='Path loss level. low/medium/high', type=str)
    adhoc_setting.add_argument('--bandwidth_max', help='The max bandwidth is the bandwidth within the range of comm_range/100 in MB/round.', type=float)
    # DataItemSettings
    dataitem_setting = parser.add_argument_group('DataItemSettings', 'Settings for DataItem')
    dataitem_setting.add_argument('--dataitem_enable', help='Enable dataitem generation.', action='store_true', default=None)  # 缺值区分
    # 显式关闭INI启用的数据项，沿用原dataitem_enable变量。
    dataitem_setting.add_argument('--no_dataitem_enable', dest='dataitem_enable', action='store_false', default=None)
    dataitem_setting.add_argument('--max_block_capacity', help='''The maximum number of dataitems that a block can contain. 
                                                                  max_block_capacity=0 will disable the dataitem mechanism.''', type=int)
    dataitem_setting.add_argument('--dataitem_size', help='The size of each dataitem in MB.', type=int)
    dataitem_setting.add_argument('--dataitem_input_interval', help='''The interval of dataitem input in rounds.
                                                                       dataitem_input_interval=0 will disable the dataitem queue of each miner.''', type=int)

    parser.add_argument('--result_path',help='The path to output results', type=str)
    parser.add_argument('--disable_post_verification', action='store_true',
                        help='Disable post verification of the longest chain in the global chain right after the main loop in exec().')

    args = vars(parser.parse_args())
    args['result_path'] = args['result_path'] and Path(args['result_path'])
    # difficulty=0 选择最大 target，仍是有效 CLI 输入。
    difficulty = args.get('difficulty')
    if difficulty is not None:
        target_bin = int('F'*64, 16) >> difficulty
        args['target'] = f"{target_bin:0>64X}"
    else:
        args['target'] = None

    if args.get('adver_lists') is not None:
        args['adver_lists'] = _literal(args.get('adver_lists', 'None'), 'adver_lists')  # 安全字面量
    args['attack_arg'] = {}
    if args.get('Ng') is not None:
        args['attack_arg']['Ng'] = args.get('Ng')
    if args.get('eclipse_target') is not None:
        args['attack_arg']['eclipse_target'] = _literal(args.get('eclipse_target'), 'eclipse_target')  # 安全字面量

    main(**args)
