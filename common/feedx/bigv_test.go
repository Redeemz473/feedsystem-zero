package feedx

import "testing"

func TestIsBigCreator(t *testing.T) {
	cases := []struct {
		name          string
		followerCount int64
		want          bool
	}{
		{name: "零粉丝", followerCount: 0, want: false},
		{name: "小V", followerCount: BigCreatorFollowerThreshold - 1, want: false},
		{name: "刚到阈值算大V", followerCount: BigCreatorFollowerThreshold, want: true},
		{name: "超过阈值", followerCount: BigCreatorFollowerThreshold + 10000, want: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsBigCreator(c.followerCount); got != c.want {
				t.Fatalf("IsBigCreator(%d) = %v, want %v", c.followerCount, got, c.want)
			}
		})
	}
}
