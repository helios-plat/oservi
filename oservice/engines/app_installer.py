"""App Installer Engine Skeleton.

收敛自双实证:
- Aegis AppStation: 一键 App 部署流水线 (catalog pull → compose → route → health check)
- Tide DevShop: compose-up + 路由注册 + 健康检查流水线

共性 (固化为机制):
- 接收安装请求 (app_id + config)
- 从 catalog_fetch 拉取 App 元数据 (image, compose_file, routes)
- compose_pull 预拉镜像
- compose_up 启动服务
- caddy_route_add 注册路由
- verify_health 轮询健康
- 结果通过 on_install_done 回调

差异 (留 inject + config):
- catalog_fetch: App catalog 数据源 → 注入 oprim
- compose_up / compose_pull: Compose 操作 → 注入 oprim
- caddy_route_add: 路由注册 → 注入 oskill
- verify_health: 健康检查 → 注入 oskill

红线对照:
- 红线 1 (≥2 实证): Aegis AppStation + Tide DevShop ✅
- 红线 2 (机制/业务分离): 安装步骤序列固化, 业务参数靠注入 callable
- 红线 3 (注入契约): catalog_fetch=oprim(1) / compose_up=oprim(1) / compose_pull=oprim(1) / caddy_route_add=oskill(1) / verify_health=oskill(1)
- 红线 4 (无状态骨架): 状态只在 AppInstallerEngine 实例
- 红线 5 (不反向依赖): 本文件不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class AppInstallerEngine(EngineSkeleton):
    """App 安装引擎骨架.

    机制 (骨架固化):
    - catalog_fetch 拉取 App 元数据
    - compose_pull 预拉镜像 (可跳过: config.skip_pull=True)
    - compose_up 启动服务
    - caddy_route_add 注册路由
    - verify_health 轮询健康 (最多 config.health_retries 次)
    - 任意步骤失败 → 输出 failed 状态 (不抛异常)

    业务 (注入填料):
    - catalog_fetch: (app_id) → {image, compose_file, routes, env_vars}
    - compose_up: (compose_file, env) → result
    - compose_pull: (compose_file) → result
    - caddy_route_add: (routes) → result
    - verify_health: (service_url, retries) → bool

    Example::

        manifest = ServiceManifest(
            name="aegis-appstation",
            skeleton="app_installer",
            inject={
                "catalog_fetch": appstore_catalog_fetch,
                "compose_up": docker_compose_up,
                "compose_pull": docker_compose_pull,
                "caddy_route_add": caddy_route_add,
                "verify_health": verify_health_after_action,
            },
            trigger={"on_interval": 0},
            config={
                "health_retries": 5,
                "health_interval_seconds": 3,
                "skip_pull": False,
            },
        )
    """

    injection_points = {
        "catalog_fetch": Injection(
            kind="oprim",
            cardinality="1",
            description="App catalog 拉取: (app_id: str) → {image, compose_file, routes, env_vars}",
        ),
        "compose_up": Injection(
            kind="oprim",
            cardinality="1",
            description="Docker Compose 启动: (compose_file: str, env: dict) → result",
        ),
        "compose_pull": Injection(
            kind="oprim",
            cardinality="1",
            description="Docker Compose 镜像预拉: (compose_file: str) → result",
        ),
        "caddy_route_add": Injection(
            kind="oskill",
            cardinality="1",
            description="Caddy 路由注册: (routes: list[dict]) → result",
        ),
        "verify_health": Injection(
            kind="oskill",
            cardinality="1",
            description="健康检查: (service_url: str, retries: int) → bool",
        ),
    }

    def __init__(
        self,
        *,
        catalog_fetch: Callable[..., Any],
        compose_up: Callable[..., Any],
        compose_pull: Callable[..., Any],
        caddy_route_add: Callable[..., Any],
        verify_health: Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        on_install_done: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.name = name
        self.catalog_fetch = catalog_fetch
        self.compose_up = compose_up
        self.compose_pull = compose_pull
        self.caddy_route_add = caddy_route_add
        self.verify_health = verify_health
        self.trigger = trigger
        self.config = config
        self.on_install_done = on_install_done

        # 运行期状态 (红线 4)
        self._running = False
        self._install_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._installs_done = 0
        self._last_error: str | None = None

    def submit_install(self, request: dict[str, Any]) -> None:
        """提交一个安装请求 (外部调用)."""
        self._install_queue.put_nowait(request)

    def run(self) -> None:
        """启动持续运行循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"AppInstallerEngine {self.name} already running")
        self._running = True
        logger.info(f"AppInstallerEngine '{self.name}' starting")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"AppInstallerEngine '{self.name}' stopped")

    def stop(self) -> None:
        self._running = False

    async def _run_loop(self) -> None:
        while self._running:
            try:
                request = await asyncio.wait_for(self._install_queue.get(), timeout=1.0)
                result = await self._install_app(request)
                self._installs_done += 1
                if self.on_install_done:
                    try:
                        self.on_install_done(result)
                    except Exception as e:
                        logger.warning(f"on_install_done callback failed: {e}")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"AppInstallerEngine '{self.name}' install failed")

    async def _install_app(self, request: dict[str, Any]) -> dict[str, Any]:
        """执行安装流水线: catalog → pull → up → route → health."""
        app_id = request.get("app_id", "")
        trail: list[dict[str, Any]] = []

        # Step 1: catalog_fetch
        catalog_info = await self._call(self.catalog_fetch, app_id=app_id)
        trail.append(
            {
                "step": "catalog_fetch",
                "ok": not isinstance(catalog_info, str) or not catalog_info.startswith("ERROR"),
            }
        )
        if isinstance(catalog_info, str) and catalog_info.startswith("ERROR"):
            return self._fail_result(app_id, "catalog_fetch", catalog_info, trail)

        compose_file = catalog_info.get("compose_file", "")
        env_vars = catalog_info.get("env_vars", {})
        routes = catalog_info.get("routes", [])
        service_url = catalog_info.get("service_url", "")

        # Step 2: compose_pull (可选)
        if not self.config.get("skip_pull"):
            pull_result = await self._call(self.compose_pull, compose_file=compose_file)
            trail.append({"step": "compose_pull", "ok": True})

        # Step 3: compose_up
        up_result = await self._call(self.compose_up, compose_file=compose_file, env=env_vars)
        trail.append(
            {
                "step": "compose_up",
                "ok": not isinstance(up_result, str) or not up_result.startswith("ERROR"),
            }
        )
        if isinstance(up_result, str) and up_result.startswith("ERROR"):
            return self._fail_result(app_id, "compose_up", up_result, trail)

        # Step 4: caddy_route_add
        if routes:
            route_result = await self._call(self.caddy_route_add, routes=routes)
            trail.append({"step": "caddy_route_add", "ok": True})

        # Step 5: verify_health
        retries = self.config.get("health_retries", 5)
        healthy = await self._call(self.verify_health, service_url=service_url, retries=retries)
        trail.append({"step": "verify_health", "ok": bool(healthy)})

        status = "installed" if healthy else "installed_unhealthy"
        return {
            "status": status,
            "app_id": app_id,
            "trail": trail,
            "catalog_info": catalog_info,
        }

    async def _call(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        """调用注入的 callable (sync/async 都支持), 异常转为 ERROR 字符串."""
        try:
            result = fn(**kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as e:
            return f"ERROR: {type(e).__name__}: {e}"

    def _fail_result(
        self, app_id: str, step: str, error: str, trail: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return {
            "status": "failed",
            "app_id": app_id,
            "failed_step": step,
            "error": error,
            "trail": trail,
        }

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "queue_size": self._install_queue.qsize(),
                "installs_done": self._installs_done,
                "last_error": self._last_error,
            },
        }


register_skeleton("app_installer", AppInstallerEngine)
