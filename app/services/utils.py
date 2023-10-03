

def find_config_for_action(configurations, action_id):
    return next(
        (
            config for config in configurations
            if config.action.value == action_id
        ),
        None
    )
