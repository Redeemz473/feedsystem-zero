package feedx

import "testing"

func TestTimelineMemberLexicographicalOrder(t *testing.T) {
	older, err := EncodeTimelineMember(1700000000000, 99)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := EncodeTimelineMember(1700000000001, 1)
	if err != nil {
		t.Fatal(err)
	}
	sameTimeLargerID, err := EncodeTimelineMember(1700000000001, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !(sameTimeLargerID > newer && newer > older) {
		t.Fatalf("unexpected lexicographical order: %q %q %q", sameTimeLargerID, newer, older)
	}
}

func TestTimelineMemberRoundTrip(t *testing.T) {
	member, err := EncodeTimelineMember(1700000000123, 123456789)
	if err != nil {
		t.Fatal(err)
	}
	publishedAt, videoID, err := DecodeTimelineMember(member)
	if err != nil {
		t.Fatal(err)
	}
	if publishedAt != 1700000000123 || videoID != 123456789 {
		t.Fatalf("unexpected decoded member: published_at=%d video_id=%d", publishedAt, videoID)
	}
}

func TestTimelineLexMax(t *testing.T) {
	first, err := TimelineLexMax(0, 0)
	if err != nil || first != "+" {
		t.Fatalf("unexpected first page max: max=%q err=%v", first, err)
	}
	next, err := TimelineLexMax(1700000000000, 8)
	if err != nil || len(next) == 0 || next[0] != '(' {
		t.Fatalf("unexpected next page max: max=%q err=%v", next, err)
	}
	if _, err := TimelineLexMax(1700000000000, 0); err == nil {
		t.Fatal("expected incomplete cursor error")
	}
}

func TestDecodeTimelineMemberRejectsInvalidValue(t *testing.T) {
	invalid := []string{"", "1:2", "0000000000000000000:00000000000000000001", "0000000001700000000-00000000000000000001"}
	for _, member := range invalid {
		if _, _, err := DecodeTimelineMember(member); err == nil {
			t.Fatalf("expected invalid member error: %q", member)
		}
	}
}
