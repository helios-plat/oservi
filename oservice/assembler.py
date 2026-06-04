"""装配器 — Manifest → Service 实例.

v0.1 朴素契约:
- 一次性装配, 不做 reconcile (无项目实证, 砍掉 K8s Operator 幂等)
- 不管部署 (容器化交项目 Docker 流程)
- 单一职责: 解析 Manifest → 校验注入 → 加载骨架 → 注入元素 → 返回 Service

红线对应:
- 红线 3 (注入类型契约): 装配前强制校验 kind + cardinality
"""

from typing import Callable, Any
from oservice.manifest import ServiceManifest, ManifestValidationError
from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    get_skeleton,
)


def _detect_element_kind(callable_obj: Callable[..., Any]) -> str:
    """检测 callable 来自哪个层 (oprim/oskill/omodul/obase).
    
    通过 __module__ 前缀判定. 校验失败返回 "unknown".
    """
    module = getattr(callable_obj, "__module__", "")
    if not module:
        return "unknown"
    
    top_pkg = module.split(".")[0]
    if top_pkg in ("oprim", "oskill", "omodul", "obase"):
        return top_pkg
    return "unknown"


def _validate_injection(
    point_name: str,
    point: Injection,
    refs: list[Callable[..., Any]] | None,
) -> None:
    """校验单个注入点是否符合 kind + cardinality 要求.
    
    Raises:
        ManifestValidationError: 校验失败
    """
    refs = refs or []
    
    # cardinality 校验
    count = len(refs)
    card = point.cardinality
    
    if card == "1":
        if count != 1:
            raise ManifestValidationError(
                f"injection '{point_name}' requires cardinality=1, got {count}"
            )
    elif card == "0..1":
        if count > 1:
            raise ManifestValidationError(
                f"injection '{point_name}' requires cardinality=0..1, got {count}"
            )
    elif card == "1..n":
        if count < 1:
            raise ManifestValidationError(
                f"injection '{point_name}' requires cardinality=1..n, got {count}"
            )
    # "0..n" 无限制
    
    # kind 类型校验 (每个 ref 的来源层必须匹配 point.kind)
    for i, ref in enumerate(refs):
        actual_kind = _detect_element_kind(ref)
        if actual_kind != point.kind:
            raise ManifestValidationError(
                f"injection '{point_name}'[{i}]: expected kind='{point.kind}', "
                f"got '{actual_kind}' (callable: {ref!r})"
            )


def validate_manifest(manifest: ServiceManifest) -> None:
    """装配前校验 Manifest 合规性.
    
    校验项:
    1. skeleton 已注册
    2. 每个注入点的 refs 满足 cardinality
    3. 每个 ref 的层级匹配 point.kind
    
    Raises:
        ManifestValidationError: 任一校验失败
    """
    # 1. 骨架已注册
    try:
        skeleton_cls = get_skeleton(manifest.skeleton)
    except KeyError as e:
        raise ManifestValidationError(str(e)) from e
    
    # 2 + 3. 注入点校验
    declared_points = skeleton_cls.injection_points
    
    for point_name, point in declared_points.items():
        refs = manifest.inject.get(point_name)
        _validate_injection(point_name, point, refs)
    
    # 4. Manifest.inject 不能含骨架未声明的注入点 (防 typo)
    for point_name in manifest.inject:
        if point_name not in declared_points:
            raise ManifestValidationError(
                f"injection '{point_name}' not declared in skeleton "
                f"'{manifest.skeleton}'. Declared: {list(declared_points.keys())}"
            )


def assemble(manifest: ServiceManifest) -> EngineSkeleton:
    """装配 Manifest → Service 实例.
    
    Args:
        manifest: 服务声明
    
    Returns:
        EngineSkeleton 实例 (可 .run() 启动)
    
    Raises:
        ManifestValidationError: Manifest 校验失败
    
    Example:
        >>> manifest = ServiceManifest(name="alerter-1", skeleton="alerter", ...)
        >>> service = assemble(manifest)
        >>> service.run()  # 阻塞启动
    """
    # 红线 3: 装配前校验
    validate_manifest(manifest)
    
    # 加载骨架
    skeleton_cls = get_skeleton(manifest.skeleton)
    
    # 构造 Service 实例 (骨架 __init__ 接收注入参数 + trigger + config)
    service = skeleton_cls(
        **manifest.inject,
        trigger=manifest.trigger,
        config=manifest.config,
        name=manifest.name,
    )
    
    return service
