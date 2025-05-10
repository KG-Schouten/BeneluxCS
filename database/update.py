## This file will be run regularly to update the database with the new data

from database.db_up import update_data

hub_id = "801f7e0c-1064-4dd1-a960-b2f54f8b5193"

update_data("new", "hub", event_id=hub_id)
update_data("new", "esea")
