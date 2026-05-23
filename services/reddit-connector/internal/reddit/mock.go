package reddit

import (
	"context"
	"time"
)

// MockClient returns synthetic posts without hitting the Reddit API.
// Use it with --mock for local dev when no OAuth credentials are available.
type MockClient struct{}

func (m *MockClient) FetchNew(_ context.Context, subreddit, _ string) ([]postData, string, error) {
	now := float64(time.Now().Unix())
	return []postData{
		{
			Name: "t3_mock001", ID: "mock001",
			Author: "mock_user_a", Title: "Elden Ring Shadow of the Erdtree is a masterpiece",
			Score: 4200, NumComments: 312, CreatedUTC: now,
			Subreddit: subreddit,
		},
		{
			Name: "t3_mock002", ID: "mock002",
			Author: "mock_user_b", Title: "Baldur's Gate 3 just hit my top 5 games of all time",
			Score: 1850, NumComments: 97, CreatedUTC: now - 60,
			Subreddit: subreddit,
		},
		{
			Name: "t1_mock003", ID: "mock003",
			Author: "mock_user_c", Body: "Hades 2 early access feels more polished than most finished games",
			Score: 890, CreatedUTC: now - 120,
			Subreddit: subreddit, ParentID: "t3_mock002",
		},
	}, "", nil
}
