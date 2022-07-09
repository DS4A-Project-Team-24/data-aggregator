CREATE TABLE IF NOT EXISTS last_fm (
  name varchar,
  duration integer,
  listeners integer,
  mbid varchar,
  url varchar,
  artist_name varchar,
  artist_mbid varchar,
  artist_url varchar,
  attr_rank varchar,
  streamable_text varchar,
  streamable_fulltrack varchar
)

CREATE TABLE IF NOT EXISTS shazam (
  rank integer,
  artist varchar,
  listeners varchar
)

CREATE TABLE IF NOT EXISTS spotify (
  track_id varchar,
  track_name varchar,
  track_popularity integer,
  artist_id varchar,
  artist_name varchar,
  artist_popularity integer,
  album varchar,
  artist_genres varchar,
  artist_followers integer,
  explicit varchar,
  danceability integer,
  energy integer,
  key integer,
  loudness integer,
  mode integer,
  speechiness integer,
  acousticness integer,
  instrumentalness integer,
  liveness integer,
  valence integer,
  tempo integer,
  uri varchar,
  analysis_url varchar,
  duration_ms integer,
  time_signature integer,
  track_href varchar,
  type varchar
)
