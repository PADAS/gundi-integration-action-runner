from gundi_action_runner import create_app

app = create_app(handlers_modules=["reference_connector.handlers"])
