import request from "./request";
import type {
  GetProfileResp,
  LoginReq,
  LoginResp,
  LogoutResp,
  RegisterReq,
  RegisterResp,
  UpdateProfileReq,
  UpdateProfileResp,
  VerificationReq,
  VerificationResp,
} from "@/types/api";

// 发送邮箱验证码
export const sendVerification = (body: VerificationReq) =>
  request.post<VerificationResp>("/account/verification", body).then((r) => r.data);

// 注册
export const register = (body: RegisterReq) =>
  request.post<RegisterResp>("/account/register", body).then((r) => r.data);

// 登录
export const login = (body: LoginReq) =>
  request.post<LoginResp>("/account/login", body).then((r) => r.data);

// 登出
export const logout = () =>
  request.post<LogoutResp>("/account/logout", {}).then((r) => r.data);

// 获取当前用户资料
export const getProfile = () =>
  request.get<GetProfileResp>("/account/profile").then((r) => r.data);

// 更新用户资料
export const updateProfile = (body: UpdateProfileReq) =>
  request.put<UpdateProfileResp>("/account/profile", body).then((r) => r.data);
