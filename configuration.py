import yaml

def merge(user, default):
    if isinstance(user,dict) and isinstance(default,dict):
        for k,v in default.iteritems():
            if k not in user:
                user[k] = v
            else:
                user[k] = merge(user[k],v)
    return user

class Configuration():
    def __init__(self):
        basefile = self.loadfile('configuration.yaml')
        privatefile = self.loadfile('configuration.private.yaml')

        self.settings = merge(privatefile, basefile)

    def loadfile(self, path):
        with open(path,'r') as f:
            return yaml.load(f.read())

if __name__ == "__main__":
    conf = Configuration()
    print conf.settings
