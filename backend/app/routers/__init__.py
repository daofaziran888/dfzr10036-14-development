"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

late_window 必须排在 resource 之前：资源路由含 ``/api/{module}/{entry_id}``
动态段，先注册会把 ``/api/late-window/state`` 这类固定路径抢先吃掉。
"""
from __future__ import annotations

from app.routers import late_window, resource

ROUTERS = [late_window, resource]
