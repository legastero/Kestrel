import os
import ConfigParser

def load_config(conf_file):
    config = {}
    parser = ConfigParser.ConfigParser()

    try:
        parser.read(os.path.expanduser(conf_file))
    except ConfigParser.MissingSectionHeaderError:
        sys.stderr.write('ERROR: Configuration file is invalid.\n')
        sys.exit()

    sections = parser.sections()
    for section in sections:
        config[section] = {}
        options = parser.options(section)
        for option in options:
            config[section][option] = parser.get(section, option)
    return config
