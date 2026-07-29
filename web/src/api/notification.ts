import request from "./request";
import type {
  GetUnreadCountResp,
  ListNotificationsReq,
  ListNotificationsResp,
  MarkAllNotificationsReadResp,
  MarkNotificationReadResp,
} from "@/types/api";

export const listNotifications = (params: ListNotificationsReq = {}) =>
  request.get<ListNotificationsResp>("/notification/", { params }).then((r) => r.data);

export const getUnreadCount = () =>
  request.get<GetUnreadCountResp>("/notification/unread-count").then((r) => r.data);

export const markNotificationRead = (notificationID: number) =>
  request
    .post<MarkNotificationReadResp>(`/notification/${notificationID}/read`)
    .then((r) => r.data);

export const markAllNotificationsRead = () =>
  request
    .post<MarkAllNotificationsReadResp>("/notification/read-all")
    .then((r) => r.data);
