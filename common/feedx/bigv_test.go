package feedx

import "testing"

func TestIsBigCreator(t *testing.T) {
	cases := []struct {
		name   string
		isBigV bool
		want   bool
	}{
		{name: "未升级", isBigV: false, want: false},
		{name: "已升级", isBigV: true, want: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsBigCreator(c.isBigV); got != c.want {
				t.Fatalf("IsBigCreator(%v) = %v, want %v", c.isBigV, got, c.want)
			}
		})
	}
}

func TestShouldPromoteBigCreator(t *testing.T) {
	cases := []struct {
		name          string
		followerCount int64
		currentIsBigV bool
		want          bool
	}{
		{name: "已是大V不再升级", followerCount: BigCreatorFollowerThreshold + 1, currentIsBigV: true, want: false},
		{name: "未达阈值不升级", followerCount: BigCreatorFollowerThreshold - 1, currentIsBigV: false, want: false},
		{name: "首次达到阈值升级", followerCount: BigCreatorFollowerThreshold, currentIsBigV: false, want: true},
		{name: "远超阈值仍升级", followerCount: BigCreatorFollowerThreshold + 10000, currentIsBigV: false, want: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ShouldPromoteBigCreator(c.followerCount, c.currentIsBigV); got != c.want {
				t.Fatalf("ShouldPromoteBigCreator(%d,%v) = %v, want %v", c.followerCount, c.currentIsBigV, got, c.want)
			}
		})
	}
}
