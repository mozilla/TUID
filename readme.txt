Setting up:

clone this repository and include tidservice.py in your project
Then create a TIDService() object

Purpose and Design:

The purpose of this program is assign and quickly retrive unique identifiers to revisions in the Firefox codebase in order to compare two different reivions and find which lines have changed. These unique identifiers are stored as TID tuples, which store a unique identifier, revision id, file name, line number, and an operator.

Usage:

To retrieve a list of TIDs for a given revision and file, call grab_tids. For example

self.service = TIDService()
self.service.grab_tids("/testing/geckodriver/CONTRIBUTING.md", "6162f89a4838")

Will grab all the TIDs for revision "6162f89a4838" from the file "/testing/geckodriver/CONTRIBUTING.md"

To grab TIDs for multiple files, call grab_tids_from_files(dir,files,revision). dir is the directory the files is in, files is the list of files, and revision is the revision
