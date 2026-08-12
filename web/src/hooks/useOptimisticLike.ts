import { useCallback, useEffect, useRef, useState } from "react";
import { toast } from "sonner";

import { likeVideo, unlikeVideo } from "@/api/interaction";
import { extractErrMsg } from "@/api/request";

// Backend response tolerance: if server-returned count differs from optimistic
// value by less than this threshold, we keep the optimistic value to avoid a
// jarring jump (e.g. user clicks +1 but sees +37 because others liked too).
const RECONCILE_THRESHOLD = 2;

interface Params {
  videoID: number;
  initialLiked: boolean;
  initialCount: number;
  // Whether the current viewer is logged in. When false, toggle() shows a
  // toast instead of firing the request.
  loggedIn: boolean;
  // Called after a successful request finishes. Useful for invalidating
  // downstream queries such as "my liked videos".
  onSuccess?: (liked: boolean, likesCount: number) => void;
}

interface Result {
  liked: boolean;
  likesCount: number;
  pending: boolean;
  toggle: () => void;
}

// useOptimisticLike encapsulates the "click -> instant preview -> request ->
// tolerate small drift / rollback on failure" pattern for the like button.
//
// Rationale:
// 1. UI must feel instant (< 16ms feedback) regardless of network latency.
// 2. Real like count on the server may diverge from what user sees because
//    of concurrent likes from other users; we deliberately hide small drift
//    (< RECONCILE_THRESHOLD) so the user does not perceive strange jumps.
// 3. When request fails we roll back exactly to the value before the click,
//    not to a stale server value which might already be outdated.
export function useOptimisticLike({
  videoID,
  initialLiked,
  initialCount,
  loggedIn,
  onSuccess,
}: Params): Result {
  const [liked, setLiked] = useState<boolean>(initialLiked);
  const [likesCount, setLikesCount] = useState<number>(initialCount);
  const [pending, setPending] = useState<boolean>(false);

  // Sync when the caller feeds a fresh initial value (e.g. video query
  // refetch). We only overwrite when we are NOT in the middle of a request
  // and the incoming values differ, otherwise React's re-render would clobber
  // the optimistic preview.
  const inFlightRef = useRef<boolean>(false);
  useEffect(() => {
    if (inFlightRef.current) return;
    setLiked(initialLiked);
    setLikesCount(initialCount);
  }, [initialLiked, initialCount]);

  const toggle = useCallback(() => {
    if (!loggedIn) {
      toast.info("请先登录");
      return;
    }
    if (inFlightRef.current) return; // debounce: ignore rapid double-clicks
    inFlightRef.current = true;
    setPending(true);

    // Snapshot for rollback on failure.
    const prevLiked = liked;
    const prevCount = likesCount;

    // Optimistic preview: apply the effect immediately.
    const nextLiked = !prevLiked;
    const nextCount = Math.max(0, prevCount + (nextLiked ? 1 : -1));
    setLiked(nextLiked);
    setLikesCount(nextCount);

    const call = nextLiked ? likeVideo(videoID) : unlikeVideo(videoID);
    call
      .then((data) => {
        // Reconcile with server value only if drift is large enough that the
        // user would notice inconsistency anyway (e.g. someone else liked 5
        // times in the meantime).
        const drift = Math.abs(data.likes_count - nextCount);
        const finalCount = drift >= RECONCILE_THRESHOLD ? data.likes_count : nextCount;
        setLiked(data.liked);
        setLikesCount(finalCount);
        onSuccess?.(data.liked, finalCount);
      })
      .catch((err: unknown) => {
        // Rollback to the exact state before the click, then surface error.
        setLiked(prevLiked);
        setLikesCount(prevCount);
        toast.error(extractErrMsg(err, "操作失败"));
      })
      .finally(() => {
        inFlightRef.current = false;
        setPending(false);
      });
  }, [videoID, loggedIn, liked, likesCount, onSuccess]);

  return { liked, likesCount, pending, toggle };
}
