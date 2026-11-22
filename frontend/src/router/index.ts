import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import LateWindowView from '@/views/LateWindowView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 固定路径要排在 /:moduleKey 前面，避免被动态段吃掉。
    { path: '/late-window', name: 'late-window', component: LateWindowView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
