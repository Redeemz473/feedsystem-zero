import { Route, Routes } from "react-router-dom";

import MainLayout from "@/layouts/MainLayout";
import LoginPage from "@/pages/LoginPage";
import RegisterPage from "@/pages/RegisterPage";
import HomePage from "@/pages/HomePage";
import ProfilePage from "@/pages/ProfilePage";
import ComingSoon from "@/pages/ComingSoon";
import { RequireAuth } from "@/router/RequireAuth";

export default function App() {
  return (
    <Routes>
      {/* 认证页面：不套 Layout */}
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />

      {/* 主布局：所有需要顶部导航的页面 */}
      <Route element={<MainLayout />}>
        <Route path="/" element={<HomePage />} />

        <Route
          path="/profile"
          element={
            <RequireAuth>
              <ProfilePage />
            </RequireAuth>
          }
        />

        <Route
          path="/upload"
          element={
            <RequireAuth>
              <ComingSoon name="视频上传（M2）" />
            </RequireAuth>
          }
        />
        <Route
          path="/likes"
          element={
            <RequireAuth>
              <ComingSoon name="我的喜欢（M3）" />
            </RequireAuth>
          }
        />
        <Route
          path="/videos/:id"
          element={<ComingSoon name="视频详情（M3）" />}
        />
        <Route
          path="/users/:id"
          element={<ComingSoon name="用户主页（M4）" />}
        />

        <Route path="*" element={<ComingSoon name="页面不存在" />} />
      </Route>
    </Routes>
  );
}
