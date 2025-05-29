# Betfair Data Parsing

These scripts will parse a raw betfair downloadable and empty the odds data into CSV files.  The data dump can be huge so test it by just running them in a structure where there are only a couple of bz2 files in the folder structure rather than 10 years.

It loops through all the folders starting with the year, unzips the bz2 file and then parses the json content and creates CSVs with the parsed betting data.  the generate_all_intervals script parses out time intervals in 10% increments from 0 to 100 to show how the odds moved from market open to close and then adds to the CSV.  

It then imports the data into the database for use in the model if you run 3_import_odds_from_csvs which you can skip as that is a step in the model commands.

### run:

    python 1_make_csvs.py

    python 2_generate_all_intervals.py

    python 3_import_odds_from_csvs.py

I put an example bz2 file here in the folder with the betfair structure for clarity

Year -> Month -> Day -> ID -> file.bz2 