These scripts will parse a raw betfair downloadable and empty the odds data into CSV files.  The data dump can be huge so test it with lines_sample.py first and if the CSV looks good then run lines.py over the full thing.

It loops through all the folders starting with the year, unzips the bz2 file and then parses the json content.

I put an example bz2 file here in the folder with the betfair structure for clarity

Year -> Month -> Day -> ID -> file.bz2