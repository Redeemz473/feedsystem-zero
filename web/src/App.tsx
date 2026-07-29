import { Route, Routes } from "react-router-dom";

import MainLayout from "@/layouts/MainLayout";
import LoginPage from "@/pages/LoginPage";
import RegisterPage from "@/pages/RegisterPage";
import HomePage from "@/pages/HomePage";
import ProfilePage from "@/pages/ProfilePage";
import UploadPage from "@/pages/UploadPage";
import LikesPage from "@/pages/LikesPage";
import VideoDetailPage from "@/pages/VideoDetailPage";
import UserPage from "@/pages/UserPage";
import FollowListPage from "@/pages/FollowListPage";
import NotificationsPage from "@/pages/NotificationsPage";
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
              <UploadPage />
            </RequireAuth>
          }
        />

        <Route
          path="/likes"
          element={
            <RequireAuth>
              <LikesPage />
            </RequireAuth>
          }
        />

        <Route
          path="/notifications"
          element={
            <RequireAuth>
              <NotificationsPage />
            </RequireAuth>
          }
        />

        <Route path="/videos/:id" element={<VideoDetailPage />} />
        <Route path="/users/:id" element={<UserPage />} />
        <Route path="/users/:id/followers" element={<FollowListPage />} />
        <Route path="/users/:id/followings" element={<FollowListPage />} />

        <Route path="*" element={<ComingSoon name="页面不存在" />} />
      </Route>
    </Routes>
  );
}
