library(tidyverse)
library(readxl)
library(jsonlite)
library(FSA)
library(lmtest)
library(sandwich)
library(car)


# ------------------------------ Data -------------------------------------------

rct_data <-
  read_xlsx("Documents/PhD/Big Datasets/8.20 Agency RCT/alldata.xlsx") %>%
  add_row(
    read_xlsx("Documents/PhD/Big Datasets/8.20 Agency RCT/calcubes_and_findmistakes.xlsx")
  ) %>%
  rename(
    parent_id = "Parent ID",
    game_version = "Game Version",
    child_uid = "Child UID",
    game_session_uid = "Game Session UID",
    parent_event_uid = "Parent Event UID",
    event_uid = "Event UID",
    event_type = "Event Type",
    capture_timestamp = "Capture Timestamp",
    sending_timestamp = "Sending Timestamp",
    server_timestamp = "Server Timestamp",
    event_data = "Event Data",
    capture_datetime = "Capture Date Time",
    server_datetime = "Server Date Time",
    sending_datetime = "Sending Date Time",
    lineage = "Lineage",
    client_host = "Client Host",
    namespace = "Namespace",
    project_name = "Project Name",
    session_index = "Session Index",
    timezone = "Timezone",
    maba_version = "Maba Version",
    maba_slug = "Maba Slug",
    country = "Country",
    platform = "Platform",
    language = "Language"
  ) %>% 
  dplyr::select(
    -c(game_version, game_session_uid, parent_event_uid, event_uid, 
       capture_timestamp, sending_timestamp, server_timestamp, lineage,
       client_host, namespace, project_name, session_index
    )
  ) %>% 
  dplyr::select(-c(server_datetime, sending_datetime)) %>% 
  relocate(maba_slug, .before = timezone) %>% 
  arrange(parent_id, child_uid, capture_datetime) %>% 
  mutate(
    action = 
      case_when(
        event_type == "enter_game" ~ "enter_game",
        event_type == "exit_game" ~ "exit_game",
        str_detect(event_data, 'foreground') ~ 'foreground',
        str_detect(event_data, 'background') ~ 'background',
        event_type == "enter_location" ~ "enter_location",
        event_type == "exit_location" ~ "exit_location",
        event_type == "enter_activity" ~ "begin_minigame",
        str_detect(event_data, "minigame_completion") ~ "complete_minigame",
        str_detect(event_data, "rage_quit") ~ "rage_quit",
        str_detect(event_data, "start_player_activity") ~ "start_player_activity",
        str_detect(event_data, "stop_player_activity") ~ "stop_player_activity",
        TRUE ~ NA
      )
  ) %>% 
  relocate(action, .before = event_type) %>% 
  filter(!is.na(action))


rct_data_tz <-
  rct_data %>% 
  select(-c(country, platform, language)) %>% 
  rowwise %>% 
  mutate(
    # it's a new location if we enter a location, start a minigame, or start an activity
    location_id = 
      ifelse(
        action == "enter_location",
        event_data %>% 
          fromJSON(.) %>% 
          {.$location_id},
        ifelse(
          action == "begin_minigame",
          event_data %>% 
            fromJSON(.) %>% 
            {.$activity_details} %>% 
            {.$settings} %>% 
            {.$category},
          case_when(
            str_detect(event_data, "findmistake") & action == "start_player_activity" ~ "findmistakes",
            str_detect(event_data, "calcube") & action == "start_player_activity" ~ "calcubes",
            TRUE ~ NA
          )
        )
      )
  ) %>% 
  ungroup %>% 
  mutate(
    location_id = 
      case_when(
        action %in% c("background", "exit_game") ~ "out of game",
        TRUE ~ location_id
      )
  ) %>% 
  relocate(location_id, .before = event_data) %>% 
  arrange(child_uid, capture_datetime)


rct_data_full <-
  rct_data_tz %>% 
  bind_rows(
    rct_breaks %>% 
      arrange(child_uid, capture_datetime) %>% 
      rename(
        next_datetime = next_capture_datetime,
        datetime = normalized_datetime
      ) %>% 
      mutate(action = "break")
  ) %>% 
  arrange(parent_id, child_uid, capture_datetime) %>% 
  mutate(date_start = capture_datetime) %>% 
  relocate(date_start, .before = capture_datetime) %>%
  relocate(event_data, .after = everything()) %>% 
  mutate(
    date_end = 
      ifelse(
        is.na(next_datetime),
        lead(date_start, order_by = capture_datetime, default = last(date_start)),
        next_datetime
      ) %>% 
      as_datetime,
    .by = child_uid
  ) %>% 
  relocate(date_end, .after = date_start) %>% 
  bind_rows(
    # occasionally, for out of game events, we will end up with a gap if we returned to the same location.
    # we need to add a row for each of these to represent the time spent after the time out of game
    # in that location.
    {
      mutate(
        .,
        next_start = lead(date_start, default = last(date_end, order_by = capture_datetime), order_by = capture_datetime),
        # next_datetime = lead(datetime, default = last(datetime, order_by = datetime), order_by = datetime), # this line needs to be checked.
        .by = child_uid
      ) %>% 
        # in order to do normalized datetime, would need to change this.
        filter(
          date_end < next_start,
          .by = child_uid
        ) %>%
        mutate(
          date_start = date_end,
          date_end = next_start,
          capture_datetime = date_start,
          action = "added",
          location_id = NA # Added to ensure that our logic then gives the correct location. However, it already should do this.
        ) %>%
        select(-next_start)
    }
  ) %>% 
  arrange(parent_id, child_uid, capture_datetime) %>% 
  mutate(
    duration_s =
      difftime(date_end, date_start, units = "secs") %>%
      as.integer
  ) %>%
  select(-c(next_datetime, next_event_type, gap_to_next_seconds)) %>% 
  filter(duration_s > 0) %>% # removing zero event times
  mutate(
    location_id =
      ifelse(
        action %in% c("break", "exit_game"),
        "out of game",
        location_id
      ),
    # we want to return to the previous location that we were in, not including minigames or out of game events
    location_group = cumsum(!is.na(location_id) & location_id != "out of game" & !action %in% c("start_player_activity", "begin_minigame")),
    next_location_id = lead(location_id, default = "none", order_by = date_start),
    begins = cumsum(!is.na(location_id)),
    .by = child_uid
  ) %>% 
  arrange(parent_id, child_uid, date_start) %>% 
  mutate(
    location_id =
      case_when(
        # If the next location is a login screen, then we were returned to the login screen upon reentry
        is.na(location_id) & next_location_id == "login" ~ "login",
        # If it's gameplay_router, same idea
        is.na(location_id) & next_location_id == "gameplay_router" ~ "gameplay_router",
        is.na(location_id) & (next_location_id != "login" | is.na(next_location_id)) ~ first(location_id),
        TRUE ~ location_id
      ),
    .by = c(child_uid, location_group)
  ) %>% 
  filter(begins > 0, location_group > 0) %>%
  select(-c(begins, location_group, next_location_id, event_data))


# This is when users make a decision, minigame or adventure
# For users on the no nudge branch, this automatically fulfills
nudge_quest_data <- 
  read_xlsx(
    "Documents/PhD/Big Datasets/8.20 Agency RCT/nudge_quest_complete.xlsx"
  ) %>% 
  rename(
    parent_id = "Parent ID",
    game_version = "Game Version",
    child_uid = "Child UID",
    game_session_uid = "Game Session UID",
    parent_event_uid = "Parent Event UID",
    event_uid = "Event UID",
    event_type = "Event Type",
    capture_timestamp = "Capture Timestamp",
    sending_timestamp = "Sending Timestamp",
    server_timestamp = "Server Timestamp",
    event_data = "Event Data",
    capture_datetime = "Capture Date Time",
    server_datetime = "Server Date Time",
    sending_datetime = "Sending Date Time",
    lineage = "Lineage",
    client_host = "Client Host",
    namespace = "Namespace",
    project_name = "Project Name",
    session_index = "Session Index",
    timezone = "Timezone",
    maba_version = "Maba Version",
    maba_slug = "Maba Slug",
    country = "Country",
    platform = "Platform",
    language = "Language"
  ) %>% 
  select(
    -c(game_version, game_session_uid, parent_event_uid, event_uid, 
       capture_timestamp, sending_timestamp, server_timestamp, lineage,
       client_host, namespace, project_name, session_index
    )
  ) %>% 
  select(-c(server_datetime, sending_datetime)) %>% 
  relocate(maba_slug, .before = timezone) %>% 
  arrange(parent_id, child_uid, capture_datetime) %>% 
  group_by(child_uid) %>% 
  slice_min(capture_datetime, with_ties = FALSE) %>% 
  ungroup

# Users' responses when prompted with nudge
accept <- 
  read_xlsx("Documents/PhD/Big Datasets/8.20 Agency RCT/accept.xlsx") %>% 
  rename(
    parent_id = `Parent ID`,
    game_version = `Game Version`,
    child_uid = `Child UID`,
    game_session_uid = `Game Session UID`,
    parent_event_uid = `Parent Event UID`,
    event_uid = `Event UID`,
    event_type = `Event Type`,
    capture_timestamp = `Capture Timestamp`,
    sending_timestamp = `Sending Timestamp`,
    server_timestamp = `Server Timestamp`,
    event_data = `Event Data`,
    capture_datetime = `Capture Date Time`,
    server_datetime = `Server Date Time`,
    sending_datetime = `Sending Date Time`,
    lineage = Lineage,
    client_host = `Client Host`,
    namespace = Namespace,
    project_name = `Project Name`,
    session_index = `Session Index`,
    timezone = Timezone,
    maba_version = `Maba Version`,
    maba_slug = `Maba Slug`,
    country = Country,
    platform = Platform
  ) %>% 
  select(-c(server_datetime, sending_datetime)) %>% 
  relocate(maba_slug, .before = timezone) %>% 
  arrange(parent_id, child_uid, capture_datetime) %>% 
  group_by(child_uid) %>% 
  slice_min(capture_datetime, with_ties = FALSE) %>% 
  ungroup %>% 
  # This is the current name for choosing the activity or minigame
  mutate(accept = str_detect(event_data, "minigame")) 


# Users that actually completed the minigame, so "completed the nudge"
mg_quest_data <- 
  read_xlsx(
    "Documents/PhD/Big Datasets/8.20 Agency RCT/mg_quest_complete.xlsx"
  ) %>% 
  rename(
    parent_id = "Parent ID",
    game_version = "Game Version",
    child_uid = "Child UID",
    game_session_uid = "Game Session UID",
    parent_event_uid = "Parent Event UID",
    event_uid = "Event UID",
    event_type = "Event Type",
    capture_timestamp = "Capture Timestamp",
    sending_timestamp = "Sending Timestamp",
    server_timestamp = "Server Timestamp",
    event_data = "Event Data",
    capture_datetime = "Capture Date Time",
    server_datetime = "Server Date Time",
    sending_datetime = "Sending Date Time",
    lineage = "Lineage",
    client_host = "Client Host",
    namespace = "Namespace",
    project_name = "Project Name",
    session_index = "Session Index",
    timezone = "Timezone",
    maba_version = "Maba Version",
    maba_slug = "Maba Slug",
    country = "Country",
    platform = "Platform",
    language = "Language"
  ) %>% 
  select(
    -c(game_version, game_session_uid, parent_event_uid, event_uid, 
       capture_timestamp, sending_timestamp, server_timestamp, lineage,
       client_host, namespace, project_name, session_index
    )
  ) %>% 
  select(-c(server_datetime, sending_datetime)) %>% 
  relocate(maba_slug, .before = timezone) %>% 
  arrange(parent_id, child_uid, capture_datetime) %>% 
  group_by(child_uid) %>% 
  slice_min(capture_datetime, with_ties = FALSE) %>% 
  ungroup


# Those children whose parent played for the first time in version 8.19
first <- 
  read_xlsx("Documents/PhD/Big Datasets/8.20 Agency RCT/first.xlsx") %>% 
  select(child_uid) %>% 
  distinct %>% 
  pull('child_uid')

# Accounts belonging to staff and other related parties that should be excluded
insiders <- 
  read_excel("Documents/PhD/Big Datasets/8.20 Agency RCT/parent.xlsx") %>% 
  rename(
    app_creation = `App Of Creation`,
    category = Category,
    country = Country,
    created = Created,
    parent_id = ID,
    source = Source,
    email_validated = `Email Validated`
  ) %>% 
  filter(category != 0) %>% 
  select(parent_id) %>% 
  distinct %>% 
  pull('parent_id')

children <- 
  read_excel("Documents/PhD/Big Datasets/8.20 Agency RCT/child.xlsx") %>% 
  rename(
    child_uid = UID,
    birthdate = Birthdate,
    parent_id = `Parent ID`,
    created = Created,
    icon_id = `Icon ID`,
    source = Source,
    is_insider = `Is Insider`,
    gender = Gender,
    product = Product
  ) %>% 
  mutate(
    age = time_length(difftime(today(), birthdate), "years") %>% as.integer
  )

# Finished quest means that they reached the nudge, NOT that they completed a minigame. 
# We use this only for those who do not receive a nudge as a way of confirming that they 
# made it to the right point, since the quest auto-resolves.
finished_quest <- 
  nudge_quest_data %>% 
  select(child_uid) %>% 
  distinct %>% 
  pull('child_uid')


# reached_quest is based on users accepting or rejecting the nudge
reached_quest <- 
  accept %>% 
  select(child_uid) %>% 
  distinct %>% 
  pull('child_uid')

# Removing users that are insiders in the company, did not play for the first time in this version,
# have data with no assigned branch, have multiple parent_ids associated with a child_uid (since this
# could lead to different branch assignments), or were assigned multiple branches
sample <-
  rct_data_full %>% 
  filter(!parent_id %in% insiders, child_uid %in% first) %>%
  filter(!any(is.na(maba_slug)), .by = parent_id) %>% # can only occur for users who played before this version
  filter(n_distinct(parent_id) == 1, .by = child_uid) %>% # several users had two parents
  filter(n_distinct(maba_slug) == 1, .by = parent_id) %>% # making sure that users are only exposed to one branch
  distinct(child_uid) %>% 
  pull('child_uid')

# -------------------------- Total Work and Play -------------------------

# Descriptive statistics
rct_data_full %>% 
  filter(child_uid %in% sample) %>%
  mutate(
    type = 
      case_when(
        location_id %in% c("timeline", "doors", "geo_location_guesser", "infinite_pillar") ~ "minigame",
        location_id %in% c("findmistakes", "calcubes") ~ "activity",
        location_id %in% c("out of game") ~ "out of game",
        location_id %in% c("login", "gameplay_router") ~ "transition",
        TRUE ~ "play"
      )
  ) %>% 
  filter(type != "out of game", type != "transition") %>% 
  summarise(
    total_time = sum(duration_s) / 60,
    total_work = sum(duration_s * (type %in% c('minigame', 'activity'))) / 60,
    total_minigame = sum(duration_s * (type == 'minigame')) / 60,
    total_activity = sum(duration_s * (type == 'activity')) / 60,
    total_time_not_custom = sum(duration_s * (location_id != "kid_room")) / 60,
    maba_slug = first(maba_slug),
    .by = child_uid
  ) %>%
  filter(total_time_not_custom >= 1) %>% # only looking at users that get a minute past customization
  mutate(
    reached_nudge = 
      ifelse(
        maba_slug == "onreturn_choice",
        child_uid %in% reached_quest,
        child_uid %in% finished_quest
      )
  ) %>% 
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  rename(maba = maba_slug) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  ) %>% 
  summarise(
    mean_age = mean(age),
    median_age = median(age),
    sd_age = sd(age),
    median_time = median(total_time),
    median_work = median(total_work),
    median_mg = median(total_minigame),
    median_activity = median(total_activity),
    mean_time = mean(total_time),
    mean_work = mean(total_work),
    mean_mg = mean(total_minigame),
    mean_activity = mean(total_activity),
    sd_time = sd(total_time),
    sd_work = sd(total_work),
    sd_mg = sd(total_minigame),
    sd_activity = sd(total_activity),
    count = n(),
    reached_nudge = n_distinct(child_uid[reached_nudge]),
    .by = maba
  )

# Full sample data
full_sample <- 
  rct_data_full %>% 
  filter(child_uid %in% sample) %>% 
  mutate(
    type = 
      case_when(
        location_id %in% c("timeline", "doors", "geo_location_guesser", "infinite_pillar") ~ "minigame",
        location_id %in% c("findmistakes", "calcubes") ~ "activity",
        location_id %in% c("out of game") ~ "out of game",
        location_id %in% c("login", "gameplay_router") ~ "transition",
        TRUE ~ "play"
      )
  ) %>% 
  filter(type != "out of game", type != "transition") %>% 
  summarise(
    total_time = sum(duration_s) / 60,
    total_work = sum(duration_s * (type %in% c('minigame', 'activity'))) / 60,
    total_minigame = sum(duration_s * (type == 'minigame')) / 60,
    total_activity = sum(duration_s * (type == 'activity')) / 60,
    total_time_not_custom = sum(duration_s * (location_id != "kid_room")) / 60,
    maba_slug = first(maba_slug),
    .by = child_uid
  ) %>%
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  filter(total_time_not_custom >= 1) %>% # only looking at users that get a minute past customization
  rename(maba = maba_slug) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  )

ols_full_sample_total_time <- 
  full_sample %>% 
  lm(log(total_time) ~ maba + age, data = .)

ols_full_sample_total_time %>% summary

coeftest(ols_full_sample_total_time, vcov. = vcovHC(ols_full_sample_total_time, type = "HC3"))

linearHypothesis(ols_full_sample_total_time, "mabaprescribed = mabaoptional", vcov. = vcovHC(ols_full_sample_total_time, type = "HC3"))
  

# OLS for work
ols_full_sample_total_work <- 
  full_sample %>% 
  lm(log(total_work + 1/60) ~ maba + age, data = .)

ols_full_sample_total_work %>% summary

coeftest(ols_full_sample_total_work, vcov. = vcovHC(ols_full_sample_total_work, type = "HC3"))

linearHypothesis(ols_full_sample_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(ols_full_sample_total_work, type = "HC3"))



# Now, looking just at users who reached the nudge...

# OLS on just those who reach the nudge
reach_sample <- 
  rct_data_full %>% 
  filter(child_uid %in% sample) %>% 
  mutate(
    type = 
      case_when(
        location_id %in% c("timeline", "doors", "geo_location_guesser", "infinite_pillar") ~ "minigame",
        location_id %in% c("findmistakes", "calcubes") ~ "activity",
        location_id %in% c("out of game") ~ "out of game",
        location_id %in% c("login", "gameplay_router") ~ "transition",
        TRUE ~ "play"
      )
  ) %>% 
  filter(type != "out of game", type != "transition") %>% 
  summarise(
    total_time = sum(duration_s) / 60,
    total_work = sum(duration_s * (type %in% c('minigame', 'activity'))) / 60,
    total_minigame = sum(duration_s * (type == 'minigame')) / 60,
    total_activity = sum(duration_s * (type == 'activity')) / 60,
    total_time_not_custom = sum(duration_s * (location_id != "kid_room")) / 60,
    maba_slug = first(maba_slug),
    .by = child_uid
  ) %>%
  filter(total_time_not_custom >= 1) %>% # only looking at users that get a minute past customization
  rename(maba = maba_slug) %>% 
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  filter(
    ifelse(
      maba == "onreturn_choice",
      child_uid %in% reached_quest,
      child_uid %in% finished_quest
    )
  ) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  )


reach_sample_total_time <- 
  reach_sample %>% 
  lm(log(total_time) ~ maba + age, data = .)

reach_sample_total_time %>% summary

coeftest(reach_sample_total_time, vcov. = vcovHC(reach_sample_total_time, type = "HC3"))

linearHypothesis(reach_sample_total_time, "mabaprescribed = mabaoptional", vcov. = vcovHC(reach_sample_total_time, type = "HC3"))

# OLS for work

reach_sample_total_work <- 
  reach_sample %>% 
  lm(log(total_work + 1/60) ~ maba + age, data = .)

reach_sample_total_work %>%
  summary

coeftest(reach_sample_total_work, vcov. = vcovHC(reach_sample_total_work, type = "HC3"))

linearHypothesis(reach_sample_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(reach_sample_total_work, type = "HC3"))


# Splitting into short-term and long-term
# Only looking at the time that occurs after reaching the nudge
data_after_nudge <-
  rct_data_full %>% 
  filter(child_uid %in% sample) %>% 
  mutate(
    type = 
      case_when(
        location_id %in% c("timeline", "doors", "geo_location_guesser", "infinite_pillar") ~ "minigame",
        location_id %in% c("findmistakes", "calcubes") ~ "activity",
        location_id %in% c("out of game") ~ "out of game",
        location_id %in% c("login", "gameplay_router") ~ "transition",
        TRUE ~ "play"
      )
  ) %>% 
  # only looking at users that reach the nudge
  filter(
    ifelse(
      maba_slug == "onreturn_choice",
      child_uid %in% reached_quest, # this signifies reaching the nudge for users without the nudge
      child_uid %in% finished_quest # we observe them either reject or accept the nudge
    )
  ) %>% 
  # We now add times for when users reach the nudge, their action, and then if they resolve the quest
  
  # this tells us for users on the optional branch when they accepted/rejected the nudge
  left_join(
    accept %>%
      rename(choice_datetime = capture_datetime) %>% 
      select(parent_id, child_uid, choice_datetime, accept, maba_version, maba_slug, timezone) %>% 
      group_by(child_uid) %>% 
      # the lowest time is the time that the quest was resolved
      slice_min(order_by = choice_datetime, with_ties = FALSE) %>% 
      ungroup %>% 
      select(child_uid, choice_datetime, accept),
    join_by(child_uid)
  ) %>% 
  # for users without a nudge (control or prescribed), when they reached the point
  left_join(
    nudge_quest_data %>% 
      rename(resolve_datetime = capture_datetime) %>% 
      filter(maba_slug %in% c("onreturn_nothing", "onreturn_doors")) %>% 
      select(child_uid, resolve_datetime),
    join_by(child_uid)
  ) %>% 
  # now, for users on the prescribed and optional branches, if/when they completed the following task of completing a minigame
  left_join(
    mg_quest_data %>% 
      rename(mg_datetime = capture_datetime) %>% 
      select(child_uid, mg_datetime),
    join_by(child_uid)
  ) %>% 
 rename(maba = maba_slug) %>% 
  # We make sure that these users play at least a minute more than customization
  # This should already be the case if they reach the nudge
  filter(
    sum(duration_s * (location_id != "kid_room" & !type %in% c('out of game', 'transition'))) >= 60,
    .by = child_uid
  ) %>%
  # only looking at data that happens after the user reaches the nudge
  # for events where the user reaches the nudge during it, we only take the latter part after the nudge
  mutate(
    date_start = 
      case_when(
        !str_detect(maba, "choice") & date_start < resolve_datetime & date_end >= resolve_datetime ~ resolve_datetime,
        str_detect(maba, "choice") & date_start < choice_datetime & date_end >= choice_datetime ~ choice_datetime,
        TRUE ~ date_start
      ),
    duration_s = 
      difftime(date_end, date_start, units = "secs") %>% 
      as.integer,
    datetime = date_start
  ) %>% 
  filter(
    ifelse(
      !str_detect(maba, "choice"),
      date_end >= resolve_datetime, # in this case, we just care when the nudge would be resolved
      date_end >= choice_datetime
    )
  ) %>%
  filter(type != "out of game", type != "transition")

data_short_long <- 
  data_after_nudge %>% 
  mutate(
    time_end = cumsum(duration_s),
    time_start = time_end - duration_s,
    period = 
      case_when(
        time_end <= 10 * 60 ~ "short-term",
        time_start >= 10 * 60 ~ "long-term",
        TRUE ~ "split"
      ),
    .by = child_uid
  ) %>% 
  # now, we need to divide the data into during the 10 minutes and afterwards
  bind_rows(
    # these will be the times that come after the split
    # Therefore, we need to modify the start time
    filter(., period == "split") %>% 
      mutate(
        date_start = date_start + (10*60 - time_start),
        time_start = 10 * 60,
        period = "long-term",
        duration_s = 
          difftime(date_end, date_start, units = "secs") %>% 
          as.integer,
        datetime = date_start
      )
  ) %>% 
  mutate(
    date_end = 
      ifelse(
        period == "split",
        date_start + (10*60 - time_start),
        date_end
      ) %>% 
      as_datetime,
    duration_s = difftime(date_end, date_start, units = "secs") %>% as.integer,
    time_end = 
      ifelse(
        period == "split",
        10 * 60,
        time_end
      ),
    period = 
      ifelse(
        period == "split",
        "short-term",
        period
      )
  ) %>% 
  arrange(parent_id, child_uid, date_start) %>% 
  summarise(
    total_time = sum(duration_s) / 60,
    total_work = sum(duration_s * (type %in% c('minigame', 'activity'))) / 60,
    total_minigame = sum(duration_s * (type == 'minigame')) / 60,
    total_activity = sum(duration_s * (type == 'activity')) / 60,
    maba = first(maba),
    parent_id = first(parent_id),
    .by = c(child_uid, period)
  ) %>% 
  bind_rows(
    filter(., !any(period == "long-term") | !any(period == "short-term"), .by = child_uid) %>% 
      # This ensures that if the after period is missing, we add an after period with 0 times,
      # and the same for if the before period is missing
      mutate(
        period = ifelse(period == "short-term", "long-term", "short-term"),
        total_time = 0,
        total_work = 0,
        total_minigame = 0,
        total_activity = 0
      )
  ) %>% 
  arrange(child_uid) %>% 
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  )


# OLS for short- and long-term work and play
short_term_total_work <- 
  data_short_long %>% 
  filter(period == "short-term") %>% 
  lm(log(total_work + 1/60) ~ maba + age, data = .)

short_term_total_work %>% summary

coeftest(short_term_total_work, vcov. = vcovHC(short_term_total_work, type = "HC3"))

linearHypothesis(short_term_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(short_term_total_work, type = "HC3"))



short_term_total_time <- 
  data_short_long %>% 
  filter(period == "short-term") %>% 
  lm(log(total_time + 1/60) ~ maba + age, data = .)

short_term_total_time %>% summary

coeftest(short_term_total_time, vcov. = vcovHC(short_term_total_time, type = "HC3"))

linearHypothesis(short_term_total_time, "mabaprescribed = mabaoptional", vcov. = vcovHC(short_term_total_time, type = "HC3"))


# long-term
long_term_total_work <- 
  data_short_long %>% 
  filter(period == "long-term") %>% 
  lm(log(total_work + 1/60) ~ maba + age, data = .)

long_term_total_work %>% summary

coeftest(long_term_total_work, vcov. = vcovHC(long_term_total_work, type = "HC3"))

linearHypothesis(long_term_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(long_term_total_work, type = "HC3"))



long_term_total_time <- 
  data_short_long %>% 
  filter(period == "long-term") %>% 
  lm(log(total_time + 1/60) ~ maba + age, data = .)

long_term_total_time %>% summary

coeftest(long_term_total_time, vcov. = vcovHC(long_term_total_time, type = "HC3"))

linearHypothesis(long_term_total_time, "mabaprescribed = mabaoptional", , vcov. = vcovHC(long_term_total_time, type = "HC3"))


# -------------------------- D1 and W0 --------------------------------------------

# Full sample
rct_data_summarized <-
  rct_data_full %>% 
  filter(child_uid %in% sample) %>%
  arrange(parent_id, child_uid, date_start) %>% 
  # ensures we only look at users who get past customization significantly
  filter(
    sum(duration_s * (!location_id %in% c('out of game', 'login', 'gameplay_router', 'kid_room'))) >= 60,
    .by = child_uid
  ) %>%
  mutate(
    type = 
      case_when(
        location_id %in% c("timeline", "doors", "geo_location_guesser", "infinite_pillar", "findmistakes", "calcubes") ~ "work",
        location_id %in% c("out of game") ~ "out of game",
        location_id %in% c("login", "gameplay_router") ~ "transition",
        TRUE ~ "play"
      )
  ) %>% 
  mutate( # consolidating similar types of content
    group = cumsum(type != lag(type, order_by = date_start, default = "none")),
    .by = child_uid
  ) %>% 
  rowwise %>% 
  mutate(
    date_start = 
      map2(
        date_start, 
        timezone, 
        ~ as_datetime(with_tz(.x, tz = .y))
      ),
    date_end = 
      map2(
        date_end, 
        timezone, 
        ~ as_datetime(with_tz(.x, tz = .y))
      )
  ) %>% 
  unnest(c(date_start, date_end)) %>%
  summarise(
    type = first(type),
    date_start = min(date_start),
    date_end = max(date_end),
    duration_s = sum(duration_s),
    maba_slug = first(maba_slug),
    .by = c(parent_id, child_uid, group)
  ) %>% 
  select(-group)


# D1 and W0 ratios
rct_data_summarized %>% 
  mutate(
    session = cumsum(type == "out of game" & duration_s > 10*60),
    .by = child_uid
  ) %>% 
  filter(type != "out of game", type != "transition") %>%
  summarise(
    session_day = first(date(date_start)),
    maba_slug = first(maba_slug),
    .by = c(child_uid, session)
  ) %>% 
  rename(maba = maba_slug) %>% 
  arrange(child_uid, session_day) %>% 
  summarise(
    D1 = any(session_day == min(session_day) + 1),
    W0 = any(session_day < min(session_day) + 7 & session_day != min(session_day)),
    maba = first(maba),
    # Checking if D1 and W0 can still be updated after the sample
    in_progress_D1 = (min(session_day) >= as_date('2025-09-01')),
    in_progress_W0 = (min(session_day) > as_date('2025-09-01') - 7),
    .by = child_uid
  ) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  summarise(
    D1_ratio = mean(D1[!in_progress_D1]),
    W0_ratio = mean(W0[!in_progress_W0]),
    count_D1 = n_distinct(child_uid[!in_progress_D1]),
    count_W0 = n_distinct(child_uid[!in_progress_W0]),
    .by = maba
  )


D1_logit_full <- 
  rct_data_summarized %>% 
  mutate(
    session = cumsum(type == "out of game" & duration_s > 10*60),
    .by = child_uid
  ) %>% 
  filter(type != "out of game", type != "transition") %>%
  summarise(
    session_day = first(date(date_start)),
    maba_slug = first(maba_slug),
    .by = c(child_uid, session)
  ) %>% 
  rename(maba = maba_slug) %>% 
  arrange(child_uid, session_day) %>% 
  summarise(
    D1 = any(session_day == min(session_day) + 1),
    W0 = any(session_day < min(session_day) + 7 & session_day != min(session_day)),
    maba = first(maba),
    # Checking if D1 and W0 can still be updated after the sample
    in_progress_D1 = (min(session_day) >= as_date('2025-09-01')),
    in_progress_W0 = (min(session_day) > as_date('2025-09-01') - 7),
    .by = child_uid
  ) %>% 
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  filter(!in_progress_D1) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  ) %>% 
  glm(D1 ~ maba + age, data = ., family = binomial())

D1_logit_full %>% summary

coeftest(D1_logit_full, vcov. = vcovHC(D1_logit_full, type = "HC3"))

linearHypothesis(D1_logit_full, "mabaprescribed = mabaoptional", vcov. = vcovHC(D1_logit_full, type = "HC3"))
  


W0_logit_full <- 
  rct_data_summarized %>% 
  mutate(
    session = cumsum(type == "out of game" & duration_s > 10*60),
    .by = child_uid
  ) %>% 
  filter(type != "out of game", type != "transition") %>%
  summarise(
    session_day = first(date(date_start)),
    maba_slug = first(maba_slug),
    .by = c(child_uid, session)
  ) %>% 
  rename(maba = maba_slug) %>% 
  arrange(child_uid, session_day) %>% 
  summarise(
    D1 = any(session_day == min(session_day) + 1),
    W0 = any(session_day < min(session_day) + 7 & session_day != min(session_day)),
    maba = first(maba),
    # Checking if D1 and W0 can still be updated after the sample
    in_progress_D1 = (min(session_day) >= as_date('2025-09-01')),
    in_progress_W0 = (min(session_day) > as_date('2025-09-01') - 7),
    .by = child_uid
  ) %>% 
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  filter(!in_progress_W0) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  ) %>% 
  glm(W0 ~ maba + age, data = ., family = binomial())

W0_logit_full %>% summary

coeftest(W0_logit_full, vcov. = vcovHC(W0_logit_full, type = "HC3"))

linearHypothesis(W0_logit_full, "mabaprescribed = mabaoptional", vcov. = vcovHC(W0_logit_full, type = "HC3"))


# Now, restricting to users that reached the nudge
rct_data_summarized_reach <-
  rct_data_full %>% 
  filter(child_uid %in% sample) %>% 
  arrange(parent_id, child_uid, date_start) %>% 
  # ensures we only look at users who get past customization significantly
  filter(
    sum(duration_s * (!location_id %in% c('out of game', 'login', 'gameplay_router', 'kid_room'))) >= 60,
    .by = child_uid
  ) %>% 
  filter(
    ifelse(
      maba_slug == "onreturn_choice",
      child_uid %in% reached_quest,
      child_uid %in% finished_quest
    ) 
  ) %>% 
  mutate(
    type = 
      case_when(
        location_id %in% c("timeline", "doors", "geo_location_guesser", "infinite_pillar", "findmistakes", "calcubes") ~ "work",
        location_id %in% c("out of game") ~ "out of game",
        location_id %in% c("login", "gameplay_router") ~ "transition",
        TRUE ~ "play"
      )
  ) %>% 
  mutate( # consolidating similar types of content
    group = cumsum(type != lag(type, order_by = date_start, default = "none")),
    .by = child_uid
  ) %>% 
  rowwise %>% 
  mutate(
    date_start = 
      map2(
        date_start, 
        timezone, 
        ~ as_datetime(with_tz(.x, tz = .y))
      ),
    date_end = 
      map2(
        date_end, 
        timezone, 
        ~ as_datetime(with_tz(.x, tz = .y))
      )
  ) %>% 
  unnest(c(date_start, date_end)) %>%
  summarise(
    type = first(type),
    date_start = min(date_start),
    date_end = max(date_end),
    duration_s = sum(duration_s),
    maba_slug = first(maba_slug),
    .by = c(parent_id, child_uid, group)
  ) %>% 
  select(-group)

rct_data_summarized_reach %>% 
  mutate(
    session = cumsum(type == "out of game" & duration_s > 10*60),
    .by = child_uid
  ) %>% 
  filter(type != "out of game", type != "transition") %>%
  summarise(
    session_day = first(date(date_start)),
    maba_slug = first(maba_slug),
    .by = c(child_uid, session)
  ) %>% 
  rename(maba = maba_slug) %>% 
  arrange(child_uid, session_day) %>% 
  summarise(
    D1 = any(session_day == min(session_day) + 1),
    W0 = any(session_day < min(session_day) + 7 & session_day != min(session_day)),
    maba = first(maba),
    # Checking if D1 and W0 can still be updated after the sample
    in_progress_D1 = (min(session_day) >= as_date('2025-09-01')),
    in_progress_W0 = (min(session_day) > as_date('2025-09-01') - 7),
    .by = child_uid
  ) %>% 
  summarise(
    D1_ratio = mean(D1[!in_progress_D1]),
    W0_ratio = mean(W0[!in_progress_W0]),
    count_D1 = n_distinct(child_uid[!in_progress_D1]),
    count_W0 = n_distinct(child_uid[!in_progress_W0]),
    .by = maba
  )


D1_logit_reach <- 
  rct_data_summarized_reach %>% 
  mutate(
    session = cumsum(type == "out of game" & duration_s > 10*60),
    .by = child_uid
  ) %>% 
  filter(type != "out of game", type != "transition") %>%
  summarise(
    session_day = first(date(date_start)),
    maba_slug = first(maba_slug),
    .by = c(child_uid, session)
  ) %>% 
  rename(maba = maba_slug) %>% 
  arrange(child_uid, session_day) %>% 
  summarise(
    D1 = any(session_day == min(session_day) + 1),
    W0 = any(session_day < min(session_day) + 7 & session_day != min(session_day)),
    maba = first(maba),
    # Checking if D1 and W0 can still be updated after the sample
    in_progress_D1 = (min(session_day) >= as_date('2025-09-01')),
    in_progress_W0 = (min(session_day) > as_date('2025-09-01') - 7),
    .by = child_uid
  ) %>% 
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  filter(!in_progress_D1) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  ) %>% 
  glm(D1 ~ maba + age, data = ., family = binomial())

D1_logit_reach %>% summary

coeftest(D1_logit_reach, vcov. = vcovHC(D1_logit_reach, type = "HC3"))

linearHypothesis(D1_logit_reach, "mabaprescribed = mabaoptional", vcov. = vcovHC(D1_logit_reach, type = "HC3"))

W0_logit_reach <- 
  rct_data_summarized_reach %>% 
  mutate(
    session = cumsum(type == "out of game" & duration_s > 10*60),
    .by = child_uid
  ) %>% 
  filter(type != "out of game", type != "transition") %>%
  summarise(
    session_day = first(date(date_start)),
    maba_slug = first(maba_slug),
    .by = c(child_uid, session)
  ) %>% 
  rename(maba = maba_slug) %>% 
  arrange(child_uid, session_day) %>% 
  summarise(
    D1 = any(session_day == min(session_day) + 1),
    W0 = any(session_day < min(session_day) + 7 & session_day != min(session_day)),
    maba = first(maba),
    # Checking if D1 and W0 can still be updated after the sample
    in_progress_D1 = (min(session_day) >= as_date('2025-09-01')),
    in_progress_W0 = (min(session_day) > as_date('2025-09-01') - 7),
    .by = child_uid
  ) %>% 
  left_join(
    children %>% 
      select(child_uid, age) %>% 
      distinct,
    join_by(child_uid)
  ) %>% 
  filter(!in_progress_W0) %>% 
  mutate(
    maba =
      ifelse(
        str_detect(maba, "nothing"),
        "control",
        ifelse(
          str_detect(maba, "choice"),
          "optional",
          "prescribed"
        )
      )
  ) %>%
  mutate(
    maba =
      as_factor(maba) %>%
      relevel(ref = "control")
  ) %>% 
  glm(W0 ~ maba + age, data = ., family = binomial())

W0_logit_reach %>% summary

coeftest(W0_logit_reach, vcov. = vcovHC(W0_logit_reach, type = "HC3"))

linearHypothesis(W0_logit_reach, "mabaprescribed = mabaoptional", vcov. = vcovHC(W0_logit_reach, type = "HC3"))

# ------------------------- p-value Adjustments ----------------------------------


# [H1] Prescribed learning will have less total time that users spend in-game than the other two branches, 
# while optional learning will have no significant difference from the control condition in total time in-game. 
# Prescribed learning will have lower total time in-game both in the short-term and in the long-term than the 
# other two branches. 

p.adjust(
  c(
    coeftest(ols_full_sample_total_time, vcov. = vcovHC(ols_full_sample_total_time, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(ols_full_sample_total_time, vcov. = vcovHC(ols_full_sample_total_time, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(ols_full_sample_total_time, "mabaprescribed = mabaoptional", vcov. = vcovHC(ols_full_sample_total_time, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(reach_sample_total_time, vcov. = vcovHC(reach_sample_total_time, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(reach_sample_total_time, vcov. = vcovHC(reach_sample_total_time, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(reach_sample_total_time, "mabaprescribed = mabaoptional", vcov. = vcovHC(reach_sample_total_time, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(short_term_total_time, vcov. = vcovHC(short_term_total_time, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(short_term_total_time, vcov. = vcovHC(short_term_total_time, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(short_term_total_time, "mabaprescribed = mabaoptional", vcov. = vcovHC(short_term_total_time, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(long_term_total_time, vcov. = vcovHC(long_term_total_time, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(long_term_total_time, vcov. = vcovHC(long_term_total_time, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(long_term_total_time, "mabaprescribed = mabaoptional", , vcov. = vcovHC(long_term_total_time, type = "HC3"))[["Pr(>F)"]][2]
  ),
  method = "holm"
)

# [H2] Prescribed learning will have a higher time spent learning in the short-term than optional learning, 
# while optional learning will also have a significantly higher time spent learning than the control condition 
# in this time period. However, prescribed learning will lead to a lower long-term time spent learning, so 
# optional learning and the control condition will have more time spent learning in this period. Overall, 
# optional learning will result in the most time spent learning, followed by the control condition, followed 
# by prescribed learning. 

p.adjust(
  c(
    coeftest(ols_full_sample_total_work, vcov. = vcovHC(ols_full_sample_total_work, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(ols_full_sample_total_work, vcov. = vcovHC(ols_full_sample_total_work, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(ols_full_sample_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(ols_full_sample_total_work, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(reach_sample_total_work, vcov. = vcovHC(reach_sample_total_work, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(reach_sample_total_work, vcov. = vcovHC(reach_sample_total_work, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(reach_sample_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(reach_sample_total_work, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(short_term_total_work, vcov. = vcovHC(short_term_total_work, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(short_term_total_work, vcov. = vcovHC(short_term_total_work, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(short_term_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(short_term_total_work, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(long_term_total_work, vcov. = vcovHC(long_term_total_work, type = "HC3"))["mabaprescribed", "Pr(>|t|)"],
    coeftest(long_term_total_work, vcov. = vcovHC(long_term_total_work, type = "HC3"))["mabaoptional", "Pr(>|t|)"],
    linearHypothesis(long_term_total_work, "mabaprescribed = mabaoptional", vcov. = vcovHC(long_term_total_work, type = "HC3"))[["Pr(>F)"]][2]
  ),
  method = "holm"
)


# [H3] Prescribed learning will decrease D1 and W0 compared to both other branches. However, there will be no 
# significant differences in D1 and W0 between the control condition and optional learning branches, since users 
# can choose what they would like to do.

p.adjust(
  c(
    coeftest(D1_logit_full, vcov. = vcovHC(D1_logit_full, type = "HC3"))["mabaprescribed", "Pr(>|z|)"],
    coeftest(D1_logit_full, vcov. = vcovHC(D1_logit_full, type = "HC3"))["mabaoptional", "Pr(>|z|)"],
    linearHypothesis(D1_logit_full, "mabaprescribed = mabaoptional", vcov. = vcovHC(D1_logit_full, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(W0_logit_full, vcov. = vcovHC(W0_logit_full, type = "HC3"))["mabaprescribed", "Pr(>|z|)"],
    coeftest(W0_logit_full, vcov. = vcovHC(W0_logit_full, type = "HC3"))["mabaoptional", "Pr(>|z|)"],
    linearHypothesis(W0_logit_full, "mabaprescribed = mabaoptional", vcov. = vcovHC(W0_logit_full, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(D1_logit_reach, vcov. = vcovHC(D1_logit_reach, type = "HC3"))["mabaprescribed", "Pr(>|z|)"],
    coeftest(D1_logit_reach, vcov. = vcovHC(D1_logit_reach, type = "HC3"))["mabaoptional", "Pr(>|z|)"],
    linearHypothesis(D1_logit_reach, "mabaprescribed = mabaoptional", vcov. = vcovHC(D1_logit_reach, type = "HC3"))[["Pr(>F)"]][2],
    
    coeftest(W0_logit_reach, vcov. = vcovHC(W0_logit_reach, type = "HC3"))["mabaprescribed", "Pr(>|z|)"],
    coeftest(W0_logit_reach, vcov. = vcovHC(W0_logit_reach, type = "HC3"))["mabaoptional", "Pr(>|z|)"],
    linearHypothesis(W0_logit_reach, "mabaprescribed = mabaoptional", vcov. = vcovHC(W0_logit_reach, type = "HC3"))[["Pr(>F)"]][2]
  ),
  method = "holm"
)
