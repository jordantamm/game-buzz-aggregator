package reddit

import (
	"context"
	"fmt"
	"time"
)

// MockClient returns synthetic posts without hitting the Reddit API.
// Use it with --mock for local dev when no OAuth credentials are available.
type MockClient struct{}

// mockEntry is one canned mention. Comments (parent != "") become t1_ items.
type mockEntry struct {
	author string
	text   string
	score  int64
	// replies is the comment count for posts; ignored for comments.
	replies int64
	comment bool
}

// mockEntries: 10 Elden Ring, 5 Baldur's Gate 3, and 1 for every other game
// in seed/games.csv.
var mockEntries = []mockEntry{
	// Elden Ring (10)
	{author: "mock_er_01", text: "Elden Ring Shadow of the Erdtree is a masterpiece", score: 4200, replies: 312},
	{author: "mock_er_02", text: "Just beat Malenia in Elden Ring after 200 attempts, I feel like a god", score: 3100, replies: 240},
	{author: "mock_er_03", text: "Elden Ring open world design still beats everything that came after it", score: 2650, replies: 180},
	{author: "mock_er_04", text: "Anyone else think the Elden Ring Erdtree DLC bosses are brutally hard?", score: 1400, replies: 210},
	{author: "mock_er_05", text: "Tarnished, what build are you running for your Elden Ring second playthrough?", score: 780, replies: 95},
	{author: "mock_er_06", text: "Elden Ring performance on PC is still stuttery, wish they'd fix it", score: 950, replies: 130},
	{author: "mock_er_07", text: "Elden Ring's Radahn fight has the best music in any game", score: 1720, comment: true},
	{author: "mock_er_08", text: "Elden Ring co-op summons make the hard bosses so much more fun", score: 610, comment: true},
	{author: "mock_er_09", text: "Honestly Elden Ring lore is too cryptic, I need a wiki open the whole time", score: 430, comment: true},
	{author: "mock_er_10", text: "Elden Ring is overrated, the late-game bosses are just spam fests", score: 120, comment: true},

	// Baldur's Gate 3 (5)
	{author: "mock_bg_01", text: "Baldur's Gate 3 just hit my top 5 games of all time", score: 1850, replies: 97},
	{author: "mock_bg_02", text: "BG3 Act 3 performance is rough on my machine but the story is unreal", score: 1320, replies: 88},
	{author: "mock_bg_03", text: "Baldur's Gate 3 companions are the best written party I've played with", score: 2210, replies: 150},
	{author: "mock_bg_04", text: "Finished my first Baldur's Gate 3 run as a Tav bard and I'm already restarting", score: 640, comment: true},
	{author: "mock_bg_05", text: "BG3 dice rolls hate me, failed every single persuasion check", score: 310, comment: true},

	// One each for the remaining games
	{author: "mock_enn_01", text: "Elden Ring Nightreign co-op is chaotic in the best way", score: 1500, replies: 160},
	{author: "mock_hd2_01", text: "Hades 2 early access feels more polished than most finished games", score: 890, comment: true},
	{author: "mock_hh2_01", text: "Helldivers 2 friendly fire has cost me more squad wipes than any boss", score: 2300, replies: 275},
	{author: "mock_cp_01", text: "Cyberpunk 2077 finally feels like the game they promised at launch", score: 1900, replies: 190},
	{author: "mock_tw_01", text: "The Witcher 3 still has the best side quests in any RPG", score: 2750, replies: 220},
	{author: "mock_sv_01", text: "Stardew Valley 1.6 update gave me another 100 hours of farming", score: 1600, replies: 120},
	{author: "mock_hk_01", text: "Hollow Knight remains the gold standard for metroidvania level design", score: 1450, replies: 105},
	{author: "mock_ss_01", text: "Hollow Knight Silksong hype is unreal, when is Team Cherry shipping it", score: 3800, replies: 410},
	{author: "mock_h1_01", text: "Hades has the best combat loop of any roguelike, fight me", score: 1250, replies: 140},
	{author: "mock_bal_01", text: "Balatro ruined my sleep schedule, just one more run", score: 2050, replies: 165},
	{author: "mock_pal_01", text: "Palworld got way better after the latest patch", score: 980, replies: 92},
	{author: "mock_wk_01", text: "Black Myth Wukong boss fights look incredible but that difficulty spike hurts", score: 1750, replies: 205},
	{author: "mock_ml_01", text: "Manor Lords is so relaxing until the bandits show up", score: 870, replies: 75},
	{author: "mock_ds_01", text: "Dark Souls 3 Nameless King is still one of the best fights in the series", score: 1330, replies: 118},
	{author: "mock_sk_01", text: "Sekiro finally clicked for me, parrying is so satisfying", score: 1180, replies: 101},
	{author: "mock_bb_01", text: "Bloodborne on PC when? Please Sony, I'm begging", score: 4100, replies: 380},
	{author: "mock_rd_01", text: "Red Dead Redemption 2 has the most immersive world I've ever played in", score: 2600, replies: 230},
	{author: "mock_tl_01", text: "The Last of Us Part II PC port looks great and the story hits just as hard", score: 1050, replies: 96},
}

func (m *MockClient) FetchNew(_ context.Context, subreddit, _ string) ([]postData, string, error) {
	now := float64(time.Now().Unix())
	posts := make([]postData, 0, len(mockEntries))
	for i, e := range mockEntries {
		id := fmt.Sprintf("mock%03d", i+1)
		p := postData{
			ID:         id,
			Author:     e.author,
			Score:      e.score,
			CreatedUTC: now - float64(i*60),
			Subreddit:  subreddit,
		}
		if e.comment {
			p.Name = "t1_" + id
			p.Body = e.text
			p.ParentID = "t3_mock001"
		} else {
			p.Name = "t3_" + id
			p.Title = e.text
			p.NumComments = e.replies
		}
		posts = append(posts, p)
	}
	return posts, "", nil
}
