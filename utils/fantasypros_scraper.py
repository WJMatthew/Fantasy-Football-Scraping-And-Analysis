# Importing helpful libraries for web scraping
import requests
from bs4 import BeautifulSoup
import pandas as pd


# Target data available for 2013-2017

def retrieve_targets(year_start, year_end):
    base_url = 'https://www.fantasypros.com/nfl/reports/targets-distribution'

    week_list = range(1,18)
    season_list = range(year_start, year_end+1)

    for year in season_list:

        weeks_ = []

        for week in week_list:
            url_end = '?year=' + str(year) + '&start=' + str(week) + '&end=' + str(week)

            url = '%s/%s' % (base_url, url_end)

            response = requests.get(url)

            soup = BeautifulSoup(response.text, 'lxml')
            tables = soup.find_all('table')

            df = pd.read_html(str(tables[0]))[0].copy()
            df['Week'] = week

            weeks_.append(df)

        target_df = pd.concat(weeks_[:])
        target_df['SEASON'] = year
        filename = 'fantasypros-targets-{}.csv'.format(str(year))
        target_df.to_csv(filename, index=False)


# Snap count stats available for 2016-2017

def retrieve_snap_counts(year_start, year_end):
    base_url = 'https://www.fantasypros.com/nfl/reports/snap-counts/'

    season_list = range(year_start, year_end+1)

    for year in season_list:

        url_end = '?year=' + str(year)

        url = '%s/%s' % (base_url, url_end)

        response = requests.get(url)

        soup = BeautifulSoup(response.text, 'lxml')
        tables = soup.find_all('table')

        target_df = pd.read_html(str(tables[0]))[0].copy()
        target_df['Season'] = year

        filename = 'fantasypros-snapcounts-{}.csv'.format(str(year))
        target_df.to_csv(filename, index=False)


# DST stats available for 2013-2017

def retrieve_dst_stats(year_start, year_end):
    base_url = 'https://www.fantasypros.com/nfl/stats/dst.php'

    week_list = range(1,18)
    season_list = range(year_start, year_end+1)

    for year in season_list:

        weeks_ = []

        for week in week_list:
            url_end = '?year=' + str(year) + '&week=' + str(week) + '&range=week'

            url = '%s/%s' % (base_url, url_end)

            response = requests.get(url)

            soup = BeautifulSoup(response.text, 'lxml')
            tables = soup.find_all('table')

            df = pd.read_html(str(tables[0]))[0].copy()
            df['Week'] = week

            weeks_.append(df)

        target_df = pd.concat(weeks_[:])

        col_list = list(target_df.columns)
        index = col_list.index('Player')
        col_list[index] = 'Team'
        target_df.columns = col_list
        target_df['SEASON'] = year
        target_df.drop('OWN', axis=1, inplace=True)
        filename = 'fantasypros-dst-{}.csv'.format(str(year))
        target_df.to_csv(filename, index=False)


def retrieve_fantasy_stats(year_start, year_end, week_start, week_end):
    position_list = ['qb', 'wr', 'rb', 'te', 'k', 'dst']
    base_url = 'https://www.fantasypros.com/nfl/stats'

    week_list = range(week_start, week_end+1)
    season_list = range(year_start, year_end + 1)

    for pos in position_list:

        for year in season_list:

            weeks_ = []

            for week in week_list:
                url_end = '?year=' + str(year) + '&week=' + str(week) + '&range=week'

                url = '%s/{}/%s'.format(pos) % (base_url, url_end)

                response = requests.get(url)

                soup = BeautifulSoup(response.text, 'lxml')
                tables = soup.find_all('table')

                df = pd.read_html(str(tables[0]))[0].copy()
                df['Week'] = week

                weeks_.append(df)

            target_df = pd.concat(weeks_[:])
            target_df['SEASON'] = year
            filename = 'fantasypros-{}-{}.csv'.format(pos, str(year))
            target_df.to_csv(filename, index=False)


def group_by_teams(stat_list):
    stat_list = stat_list[:]
    stat_seasons = []

    for i in range(len(stat_list)):
        by_teams = stat_list[i].groupby('Team')

        df_list = []
        for name, subdf in by_teams:
            df_list.append(subdf)
            # team = subdf['Team'].values[0]  # str

        stat_seasons.append(df_list[:])

    df_list = []
    for team in stat_seasons:  # 32

        df_list.append(pd.concat(team))

    return pd.concat(df_list)


def start_the_process():

    retrieve_targets(2013, 2017)
    retrieve_snap_counts(2016, 2017)
    retrieve_dst_stats(2013, 2017)

    targets_list, snap_list, dst_list = [], [], []

    for year in range(2013, 2018):

        if 2013 < year < 2018:
            targets_df = pd.read_csv('fantasypros-targets-{}.csv'.format(year))
            targets_list.append(targets_df)

        if 2016 < year < 2018:
            snap_df = pd.read_csv('fantasypros-snapcounts-{}.csv'.format(year))
            snap_list.append(snap_df)

        if 2013 < year < 2018:
            dst_df = pd.read_csv('fantasypros-dst-{}.csv'.format(year))
            dst_list.append(dst_df)

    all_targets = group_by_teams(targets_list)
    all_snaps = group_by_teams(snap_list)
    all_dst = group_by_teams(dst_list)

    all_targets.to_csv('fantasypros-targets-{}to{}.csv'.format(13, 17))
    all_snaps.to_csv('fantasypros-snapcounts-{}to{}.csv'.format(16, 17))
    all_dst.to_csv('fantasypros-dst-{}to{}.csv'.format(13, 17))

    retrieve_fantasy_stats(2016, 2017, 1, 18)
    # Do stuff with these, /stopped here