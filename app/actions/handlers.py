"""
Define here a handler function for each supported action. The name must start withe the prefix action_
And it must receive an integration and a configuration


from .configurations import AuthenticateConfig, PullObservationsConfig

For example, this "auth" action will authenticate against the third-party system to test credentials
async def action_auth(integration, action_config):
    # Custom logix to authenticate
    # The action handler can return a result (optional). In this case it's the result of the connection test.
    # ToDo: Standardize these responses
    return {"valid_credentials": token is not None} 

This "pull_observations" action implements the data extraction
async def action_pull_observations(integration, action_config):
    # Authenticate ..
    # Extract Data ..
    # Transform it ..
    # Push it to Gundi API v2 ..
    # Return a result (optional)
    return {"observations_extracted": 100}


You can add here any other helper functions as needed too. Just ensure that they don't start with the "action_" prefix
or they will be considered actions.

"""