import logging
from datetime import datetime, timedelta
from app.actions.configurations import AuthenticateConfig, PullEventsConfig
from app.services.errors import ConfigurationNotFound, ConfigurationValidationError
from erclient import ERClient, ERClientException
import pytz
import json
from shapely.geometry import shape
from typing import List, Dict

class er_syncer:

    SYNC_FIELDS_EVENTS = ['id', 'location', 'time', 'event_type',
        'priority', 'title', 'state', 'event_details', 'is_collection', 'geometry', 'geojson']
    SYNC_FIELDS_EVENT_TYPES = ['value', 'display', 'ordernum', 'icon_id', 'is_active', 'schema', 'properties',
                               'category', 'is_collection', 'geometry_type']


    def __init__(self, auth_config: AuthenticateConfig, action_config: PullEventsConfig):
        self.action_config = action_config
        self.auth_config = auth_config

        try:
            self.src_erclient = ERClient(service_root=auth_config.source_server, token = auth_config.source_token)
        except Exception as e:
            logging.exception(f"Could not log in to source client", e)
            raise e
        
        try:
            self.dest_erclient = ERClient(service_root=auth_config.dest_server, token = auth_config.dest_token)
        except Exception as e:
            logging.exception(f"Could not log in to destination client", e)
            raise e
        
    def sync(self, start_date):
        logging.info(f"Syncing events since '{start_date}' from '{self.src_erclient.service_root}' to '{self.dest_erclient.service_root}'")

        source_events = self._get_events(since = start_date, er_client = self.src_er_client,
            include_event_types = None, within_featuregroups = self.action_config.within_featuregroups)

        event_types_to_sync = []
        events_to_sync = []

        # Create a list of the unique event types that we're going to pull from source
        for event in source_events:
            if(event['event_type'] not in event_types_to_sync):
                event_types_to_sync.append(event['event_type'])
            events_to_sync.append(event)

        if(len(events_to_sync) > 0):
            logging.info(f"Found '{len(events_to_sync)}' events to sync from source site '{self.src_erclient.service_root}'")
            self.dest_event_types = {}
            if(len(event_types_to_sync) > 0):
                if(self.action_config.update_schema):
                    logging.info("Syncing event categories...")
                    self.sync_event_categories()
                    logging.info("Syncing event types...")
                    self.dest_event_types = self.sync_event_types(event_types_to_sync)
                else:
                    logging.info("Get destination event types...")
                    self.dest_event_types = self.get_event_types(event_types_to_sync, self.dest_er_client)

                    for et_k in event_types_to_sync:
                        dest_event_type = et_k
                        if(self.prepend_system_to_event_types):
                            dest_event_type = self.source_sys_value + "_" + dest_event_type
                        if(dest_event_type not in self.dest_event_types):
                            raise ConfigurationValidationError(f"update_schema is false, but event type {dest_event_type} is missing from destination.")
                        for prop in ["er2er_src_id", "er2er_src_serial_number", "er2er_src_system", "er2er_src_service_root"]:
                            et = self.dest_event_types[dest_event_type]
                            if(prop not in et['schema'].get('schema',{}).get('properties', {})):
                                raise ConfigurationValidationError(
                                    f"update_schema is false, but event type {dest_event_type} is missing required property {prop}")

            logging.info(f"Syncing {events_to_sync} events from source site {self.src_er_client.service_root} to destination site {self.dest_er_client.service_root}. Start date: {start_date}")
            self.sync_events(source_events = events_to_sync, start_date = start_date)

    def sync_event_types(self, event_types_to_sync:List):

        logging.info("Loading event type schemas")
        source_event_types = self.get_event_types(event_types_to_sync, self.src_er_client)

        logging.info(f"Found {len(source_event_types)} event types from source site {self.src_erclient.service_root}")

        # Grab the existing event types that match the source event types, if any
        dest_event_types_to_sync = []
        for event_type in event_types_to_sync:
            dest_event_type = event_type
            if(self.action_config.prepend_system_to_event_types):
                dest_event_type = self.action_config.source_system_abbr + "_" + dest_event_type
            dest_event_type = dest_event_type.lower()

            if(dest_event_type not in dest_event_types_to_sync):
                dest_event_types_to_sync.append(dest_event_type)

        dest_event_types = self.get_event_types(dest_event_types_to_sync, self.dest_er_client)

        logging.info(f"Found {len(dest_event_types)} event types from destination site {self.dest_erclient.service_root}")

        result_types = {}
        for event_type_name, event_type in source_event_types.items():

            if(event_type_name not in event_types_to_sync):
                logging.info(f"Skipping event type {event_type_name} because it's not in the list of event types to sync")
                continue

            new_event_type = self.clean_event_type(event_type)
            try:
                if(new_event_type['value'] in dest_event_types):
                    dest_event_type = dest_event_types[new_event_type['value']]
                    logging.info(f"Updating destination event type {new_event_type['value']} from source event type {event_type_name}")
                    new_event_type['id'] = dest_event_type["id"]
                    result_types[new_event_type['value']] = self.dest_er_client.patch_event_type(new_event_type)
                else:
                    logging.info(f"Creating destination event type {new_event_type['value']} from source event type {event_type_name}")
                    result_types[new_event_type['value']] = self.dest_er_client.post_event_type(new_event_type)
            except ERClientException as e:
                logging.exception(f"Error while creating event type {new_event_type['value']} in destination '{self.dest_er_client.service_root}'.  Exception: {e}. User: {self.dest_er_client.username}. Skipping creation...")
                continue

        return result_types

    @staticmethod
    def _get_events(self, since: datetime, erclient: ERClient, include_event_types: List[str] = [],
                         within_featuregroups: List[str] = []):

        filter = {}
        if(since != None):
            tomorrow = datetime.now(tz=pytz.utc) + timedelta(days=1)
            filter = {
                'update_date' : {
                    'lower': since.strftime("%Y-%m-%dT%H:%M:%S%z")
                }
            }

        event_type_ids = self.get_event_type_ids(erclient, include_event_types)

        containing_shapes = []
        if(within_featuregroups):
            for fg in within_featuregroups:
                logging.info(f"Loading feature group {fg} to filter events.")
                
                try:
                    featuregroup = list(erclient.get_objects(object=f"spatialfeaturegroup/{fg}"))[0]
                except (ERClientException, IndexError) as e:
                    logging.error(f"Could not load feature group {fg}: {e}")
                    raise ConfigurationValidationError(f"Could not load feature group {fg}.\nTroubleshoot by looking for this feature group in the ER system {erclient.service_root}.")
                
                for erfeature in featuregroup.get("features", []):
                    for geojsonfeature in erfeature.get("features", []):
                        if("geometry" in geojsonfeature):
                            polygon = shape(geojsonfeature["geometry"])
                            containing_shapes.append(polygon)

        for event_type_id in event_type_ids:
            if(event_type_id):
                filter['event_type'] = [event_type_id]

            msg = f"Loading events since {since.strftime('%Y-%m-%dT%H:%M:%S%z')} from {erclient.service_root} of type {event_type_id}. "
            if(within_featuregroups):
                msg += f"Restricting events to those within feature groups {within_featuregroups}."
            logging.info(msg)
            events = erclient.get_objects_multithreaded(object="activity/events",
                filter = json.dumps(filter),
                include_notes = True,
                include_related_events = False,
                include_files = True,
                include_details = True,
                include_updates = False,
                page_size = 100)

            if(not within_featuregroups):
                yield from events
            else:
                for e in events:
                    if('geojson' not in e) or not e['geojson']:
                        continue
                    event_geom = e['geojson'].get('geometry')
                    if not event_geom:
                        continue
                    event_geom = shape(event_geom)
                    for s in containing_shapes:
                        if s.intersects(event_geom):
                            yield e
                            break

    @staticmethod
    def _get_event_type_ids(erclient, event_types: List[str]) -> List[str]:
        if(not event_types):
            return []

        er_event_types = erclient.get_event_types()

        results = []
        for event_type in er_event_types:
            if(event_type and (event_type.get('value') in event_types)):
                results.append(event_type['id'])

        return results
    
    def sync_event_categories(self):

        source_cats = self._get_event_categories(self.src_erclient)
        logging.info(f"Found {len(source_cats)} event categories from source site {self.src_erclient.service_root}")

        dest_cats = self._get_event_categories(self.dest_erclient)
        logging.info(f"Found {len(dest_cats)} event categories from destination site {self.dest_erclient.service_root}")

        # For all source categories, if the destination category doesn't already exist, create it.
        # If it does already exist, update it.
        for cat in source_cats:
            dest_cat_value = cat
            if(self.action_config.prepend_system_to_categories):
                dest_cat_value = self.action_config.source_system_abbr + "_" + dest_cat_value

            if(dest_cat_value not in dest_cats):
                new_cat = self.create_dest_category(source_cats[cat])
                if new_cat:
                    dest_cats[new_cat['value']] = new_cat

            else:
                self.update_dest_category(dest_cats[dest_cat_value], source_cats[cat])

    def create_dest_category(self, cat):
        new_cat = {
            "value": cat['value'],
            "display": cat['display']
        }
        if(self.prepend_system_to_categories):
            new_cat["value"] = self.action_config.source_system_abbr + "_" + new_cat["value"]
            new_cat["display"] = self.action_config.source_system_name + "-" + new_cat["display"]

        logging.info(f"Creating destination category {new_cat['value']} from source category {cat['value']}")
        try:
            new_er_cat = self.dest_er_client.post_event_category(new_cat)
        except ERClientException as e:
            logging.warning(f"Category {new_cat['value']} might already exist in destination '{self.dest_erclient.service_root}'.  Skipping creation...")
            return None
        return new_er_cat

    def update_dest_category(self, dest_cat, src_cat):
        new_cat = {
            "id": dest_cat['id'],
            "value": src_cat['value'],
            "display": src_cat['display']
        }

        if(self.action_config.prepend_system_to_categories):
            new_cat["value"] = self.action_config.source_system_abbr + "_" + new_cat["value"]
            new_cat["display"] = self.action_config.source_system_name + "-" + new_cat["display"]

        if((dest_cat["value"] == new_cat["value"]) and (dest_cat["display"] == new_cat["display"])):
            logging.debug(f"Destination category {dest_cat['value']} hasn't changed.  Skipping.")
            return dest_cat

        logging.info(f"Updating destination category {dest_cat['value']} from source category {src_cat['value']}")
        new_er_cat = self.dest_er_client.patch_event_category(new_cat)
        return new_er_cat

    @staticmethod
    def _get_event_categories(erclient) -> Dict:
        cats = erclient.get_event_categories()

        ret_cats = {}
        for cat in cats:
            ret_cats[cat['value']] = cat
        return ret_cats
    
    @staticmethod
    def get_incident_map(events: List[Dict]) -> Dict[str, List[str]]:
        incident_map = {}
        for event in events:
            if('is_contained_in' not in event):
                continue

            for container in event['is_contained_in']:
                if(container['type'] != 'contains'):
                    continue

                serial = container['related_event']['id']
                if(serial not in incident_map):
                    incident_map[serial] = []

                incident_map[serial].append(event['id'])

        return incident_map


    def clean_schema_definition_list(self, defs):
        if(type(defs) == list):
            retdefs = []
            for defn in defs:
                if(type(defn) == dict):
                    if(defn.get("type") == "fieldset"):
                        defn['items'] = self.clean_schema_definition_list(defn.get("items"))
                    if(defn.get("type") == "checkboxes"):
                        defn["type"] = "string"
                    if("titleMap" in defn):
                        del defn["titleMap"]
                retdefs.append(defn)
            return retdefs
        return defs

    def clean_event_type(self, event_type):
        new_event_type = dict((k, v) for k, v in event_type.items() if k in self.SYNC_FIELDS_EVENT_TYPES)
        new_event_type['schema']['schema'] = self.clean_event_schema(schema=new_event_type['schema']['schema'], event_type=new_event_type['value'])
        new_event_type['schema'] = json.dumps(new_event_type['schema'], indent=4)

        # Prepend the name of the system to the event type
        if(self.action_config.prepend_system_to_event_types):
            new_event_type['value'] = self.action_config.source_system_abbr + "_" + new_event_type['value']
            new_event_type['display'] = self.action_config.source_system_name + "-" + new_event_type['display']
        new_event_type['value'] = new_event_type['value'].lower()

        new_event_type['category'] = new_event_type['category']['value']
        if(self.action_config.prepend_system_to_categories):
            new_event_type['category'] = self.action_config.source_system_abbr + "_" + new_event_type['category']

        return new_event_type
    
    def clean_event_schema(self, schema=None, event_type=None):

        schema['properties']['er2er_src_id'] = {
            "type": "string",
            "title": "Event Type Serial Number from Source ER System"
        }

        schema['properties']['er2er_src_system'] = {
            "type": "string",
            "title": "Name of Source ER System"
        }

        schema['properties']['er2er_src_serial_number'] = {
            "type": "string",
            "title": "Serial Number of Source Event"
        }

        schema['properties']['er2er_src_service_root'] = {
            "type": "string",
            "title": "Service Root of Source Event"
        }

        for prop_name, prop in schema['properties'].items():
            if('enumNames' in prop and isinstance(prop['enumNames'], list)):
                logging.warning('Source schema for event type %s has enumNames as a list.  Converting to dict.', event_type)
                prop['enumNames'] = dict( (v,v) for v in prop['enumNames'] )

        schema['readonly'] = True

        return schema



    