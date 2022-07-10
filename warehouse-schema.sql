CREATE TABLE IF NOT EXISTS last_fm (
  ds4a_id varchar primary key,
  track_name varchar,
  duration integer,
  listeners integer,
  mbid varchar,
  url varchar,
  artist_name varchar,
  artist_mbid varchar,
  artist_url varchar,
  attr_rank varchar,
  streamable_text varchar,
  streamable_fulltrack varchar,
  file_upload_date date,
  insert_timestamp timestamp default getdate()
);

CREATE TABLE IF NOT EXISTS shazam (
  ds4a_id varchar primary key,
  rank integer,
  artist varchar,
  title varchar,
  file_upload_date date,
  insert_timestamp timestamp default getdate()
);

CREATE TABLE IF NOT EXISTS spotify (
  ds4a_id varchar primary key,
  track_id varchar,
  track_name varchar,
  track_popularity integer,
  artist_id varchar,
  artist_name varchar,
  artist_popularity integer,
  album varchar,
  artist_genres varchar,
  artist_followers integer,
  explicit boolean,
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
  type varchar,
  file_upload_date date,
  insert_timestamp timestamp default getdate()
);
