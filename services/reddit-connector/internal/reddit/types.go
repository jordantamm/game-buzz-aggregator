package reddit

// listingResponse is the top-level Reddit /r/{sub}/new API response.
type listingResponse struct {
	Data listingData `json:"data"`
}

type listingData struct {
	After    string     `json:"after"`
	Children []child    `json:"children"`
}

type child struct {
	Kind string    `json:"kind"`
	Data postData  `json:"data"`
}

// postData covers both posts (kind=t3) and comments (kind=t1).
type postData struct {
	Name      string  `json:"name"`       // fullname, e.g. "t3_abc123"
	ID        string  `json:"id"`
	Author    string  `json:"author"`
	Body      string  `json:"body"`       // comment text
	Selftext  string  `json:"selftext"`   // post body
	Title     string  `json:"title"`      // post title
	Score     int64   `json:"score"`
	NumComments int64 `json:"num_comments"`
	CreatedUTC float64 `json:"created_utc"`
	Subreddit string  `json:"subreddit"`
	ParentID  string  `json:"parent_id"`
}

func (d postData) text() string {
	if d.Title != "" {
		if d.Selftext != "" {
			return d.Title + "\n\n" + d.Selftext
		}
		return d.Title
	}
	return d.Body
}
