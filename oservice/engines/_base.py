"""EngineSkeleton 基类 + 注入点声明.

引擎骨架准入 5 红线 (见 docs/GOVERNANCE.md):
1. ≥2 真实项目实证 (首要红线)
2. 机制/业务分离 (业务靠注入)
3. 注入点类型契约 (kind + cardinality)
4. 无状态骨架定义 (状态只在 Service 实例)
5. 不反向依赖 (3O 四包禁 import oservice)
"""

from dataclasses import dataclass
from typing import Literal, Any, Callable
from abc import ABC, abstractmethod


Cardinality = Literal["1", "0..1", "1..n", "0..n"]
"""注入点基数 (OSGi DS 风格).

- "1": 必须恰好一个
- "0..1": 可选单个 (缺失 → None)
- "1..n": 至少一个
- "0..n": 任意多个 (缺失 → 空列表)
"""


InjectionKind = Literal["oprim", "oskill", "omodul", "obase", "layer4"]
"""注入点接受的元素层级.

- oprim/oskill/omodul/obase: 主库 4 仓元素 (扁平命名, 跨业务复用)
- layer4: 项目服务层 callable (thin wrapper / 业务特定 CRUD / 非主库元素)
  适用场景: 引擎需要项目特定数据访问 (e.g. Stratum 订阅 CRUD DuckDB), 不入主库.
  装配器对 layer4 注入不做主库来源校验 (callable 可来自任何模块).
"""


@dataclass(frozen=True)
class Injection:
    """注入点声明.
    
    每个引擎骨架在类属性 injection_points 中声明所需的注入点.
    装配器据此校验 Manifest.inject 是否合规.
    
    Args:
        kind: 接受的元素层级 (oprim / oskill / omodul / obase)
        cardinality: 引用基数
        description: 注入点用途说明 (debug 友好)
    
    Example:
        injection_points = {
            "evaluators": Injection(kind="oprim", cardinality="1..n",
                                    description="阈值判定 oprim 列表"),
            "channels":   Injection(kind="obase", cardinality="1..n",
                                    description="推送通道"),
        }
    """
    
    kind: InjectionKind
    cardinality: Cardinality
    description: str = ""


class EngineSkeleton(ABC):
    """引擎骨架基类.
    
    所有引擎骨架必须:
    1. 继承本类
    2. 类属性声明 injection_points: dict[str, Injection]
    3. __init__ 接收注入参数 + trigger + config
    4. 实现 run() (启动持续运行循环)
    5. 实现 stop() (优雅停止)
    
    红线 4: 骨架定义无状态. 状态只在 Service 实例的 self.xxx 运行期出现.
    """
    
    # 子类必须 override 此类属性
    injection_points: dict[str, Injection] = {}
    
    @abstractmethod
    def run(self) -> None:
        """启动引擎主循环 (持续运行).
        
        阻塞函数. 调用方决定是否 asyncio.create_task / threading.Thread.
        """
        ...
    
    @abstractmethod
    def stop(self) -> None:
        """优雅停止引擎."""
        ...
    
    def health(self) -> dict[str, Any]:
        """健康检查 (默认实现, 子类可 override).
        
        Returns:
            {"status": "healthy" | "unhealthy", "details": dict}
        """
        return {"status": "healthy", "details": {}}


# 引擎骨架注册表 (装配器据此查骨架)
_SKELETON_REGISTRY: dict[str, type[EngineSkeleton]] = {}


def register_skeleton(name: str, skeleton_cls: type[EngineSkeleton]) -> None:
    """注册引擎骨架到全局 registry.
    
    Args:
        name: 骨架名 (Manifest.skeleton 字段对应)
        skeleton_cls: EngineSkeleton 子类
    
    Raises:
        ValueError: 名字已注册 (避免冲突)
        TypeError: 不是 EngineSkeleton 子类
    """
    if not issubclass(skeleton_cls, EngineSkeleton):
        raise TypeError(f"{skeleton_cls} must subclass EngineSkeleton")
    if name in _SKELETON_REGISTRY:
        raise ValueError(f"skeleton '{name}' already registered")
    _SKELETON_REGISTRY[name] = skeleton_cls


def get_skeleton(name: str) -> type[EngineSkeleton]:
    """从 registry 取骨架类.
    
    Raises:
        KeyError: 骨架未注册
    """
    if name not in _SKELETON_REGISTRY:
        raise KeyError(
            f"skeleton '{name}' not registered. "
            f"Available: {list(_SKELETON_REGISTRY.keys())}"
        )
    return _SKELETON_REGISTRY[name]


def list_skeletons() -> list[str]:
    """列出所有注册的骨架名."""
    return sorted(_SKELETON_REGISTRY.keys())
