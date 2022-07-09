CREATE TABLE IF NOT EXISTS last_fm (
  name varchar(128),
  duration integer(30),
  listeners integer(30),
  mbid varchar(128),
  url varchar(128),
  artist_name varchar(128),
  artist_mbid varchar(128),
  artist_url varchar(128),
  attr_rank varchar(128),
  streamable_text varchar(128),
  streamable_fulltrack varchar(128)
)

CREATE TABLE IF NOT EXISTS shazam (
  rank integer(30),
  artist varchar(128),
  listeners varchar(128),
)

18  artist_id          1000 non-null   object
19  artist_name        1000 non-null   object
20  album              1000 non-null   object
21  track_name         1000 non-null   object
23  track_popularity   1000 non-null   int64
24  artist_popularity  1000 non-null   int64
25  artist_genres      1000 non-null   object
26  artist_followers   1000 non-null   int64

CREATE TABLE IF NOT EXISTS spotify (
  track_id varchar(128),
  track_name varchar(128),
  track_popularity integer(30),
  artist_id varchar(128),
  artist_name varchar(128),
  artist_popularity integer(30),
  album varchar(128),
  artist_genres varchar,
  artist_followers integer(30),
  explicit varchar(128),
  danceability integer(30),
  energy integer(30),
  key integer(30),
  loudness integer(30),
  mode integer(30),
  speechiness integer(30),
  acousticness integer(30),
  instrumentalness integer(30),
  liveness integer(30),
  valence integer(30),
  tempo integer(30),
  uri varchar(128),
  analysis_url varchar(128),
  duration_ms integer(30),
  time_signature integer(30),
  track_href varchar(128),
  type varchar(128)
)
