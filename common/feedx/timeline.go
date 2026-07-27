package feedx

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	timelineTimeWidth = 19
	timelineIDWidth   = 20
	timelineSeparator = ":"
)

// EncodeTimelineMember 把发布时间和视频 ID 编成固定宽度的 ZSet member。
// Timeline 中所有元素使用相同 score=0，实际顺序由 member 的字典序决定：
// publishedAt 越大越新；同一毫秒内 videoID 越大越新。
func EncodeTimelineMember(publishedAt int64, videoID uint64) (string, error) {
	if publishedAt <= 0 {
		return "", errors.New("发布时间必须大于0")
	}
	if videoID == 0 {
		return "", errors.New("视频ID必须大于0")
	}
	return fmt.Sprintf("%0*d%s%0*d", timelineTimeWidth, publishedAt, timelineSeparator, timelineIDWidth, videoID), nil
}

// DecodeTimelineMember 解析 Timeline member，并严格拒绝格式异常的数据。
func DecodeTimelineMember(member string) (publishedAt int64, videoID uint64, err error) {
	if len(member) != timelineTimeWidth+len(timelineSeparator)+timelineIDWidth {
		return 0, 0, errors.New("Timeline member长度不正确")
	}
	if member[timelineTimeWidth:timelineTimeWidth+1] != timelineSeparator {
		return 0, 0, errors.New("Timeline member分隔符不正确")
	}

	publishedAt, err = strconv.ParseInt(member[:timelineTimeWidth], 10, 64)
	if err != nil || publishedAt <= 0 {
		return 0, 0, errors.New("Timeline member发布时间不正确")
	}
	videoID, err = strconv.ParseUint(member[timelineTimeWidth+1:], 10, 64)
	if err != nil || videoID == 0 {
		return 0, 0, errors.New("Timeline member视频ID不正确")
	}
	return publishedAt, videoID, nil
}

// TimelineLexMax 返回 ZREVRANGEBYLEX 使用的上界。
// 首页使用 "+"；后续页使用排他上界 "(member"，即从上一页最后一条之后继续。
func TimelineLexMax(cursorPublishedAt int64, cursorVideoID uint64) (string, error) {
	if cursorPublishedAt == 0 && cursorVideoID == 0 {
		return "+", nil
	}
	if cursorPublishedAt <= 0 || cursorVideoID == 0 {
		return "", errors.New("发布时间游标和视频ID游标必须同时提供")
	}
	member, err := EncodeTimelineMember(cursorPublishedAt, cursorVideoID)
	if err != nil {
		return "", err
	}
	return "(" + member, nil
}
