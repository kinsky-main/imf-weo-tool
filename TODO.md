- [ ] Switch the backend to use the (pysdmx)[https://py.sdmx.io/start.html] library instead of current pure REST API calls. This will allow us to use the SDMX registry to get the list of available datasets and their dimensions, which will make it easier to implement the above two points.
- [x] Add a progress bar or some indication that the data is being fetched after the user hits space to select an option, as it can take a while to fetch the data and it may not be clear to the user that something is happening.
- [x] Check that matches for the requested selectors exist before allowing user to proceed. Only show selectors that exist, currrently it seems after picking countries and then an indicator no options are available.
- [x] Up and down arrow keys to navigate should also switch context to the selection window, currently hitting space after moving the selector just adds a space in the search bar rather than selecting the highlighted option. 
- [x] For the weo_to_dataframe script I would like that to be a notebook rather than an export script, which shows how you would pull the data using pysdmx.
- [ ] Numbers are saved as strings in the output excel file, requiring later conversion. Switch to using the pandas excel writer, it does not have this issue.
- [ ] Review code to make it more modular and concise.
- [ ] Unit selection shows units, also seems a bit pointless as the only units are "Units: Billions of U.S. dollars" or percentages.
- [ ] Doubtful that selection is as limited as it appears to be, maybe show subset of available series when hovering over country. Also have option on start of TUI to choose whether to begin selection by country or by indicator.

