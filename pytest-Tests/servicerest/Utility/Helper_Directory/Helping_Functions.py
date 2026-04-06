import os
default_dict={}
default_dict['XA_ADMIN_PASSWORD'] = 'rangerR0cks!'
default_dict['XA_ADMIN_USERNAME'] = 'admin'
default_dict['XA_KEYADMIN_PASSWORD'] = 'rangerR0cks!'
default_dict['XA_KEYADMIN_USERNAME'] = 'keyadmin'
def getEnv(str1 ,str2) :
    if str1 in default_dict:
        return default_dict[str1]
    else:return str2





class Version:
    """A version comparison utility class for semantic versioning."""

    def __init__(self, version_string):
        """
        Initialize a Version object from a version string.

        Args:
            version_string (str): Version string like "7.3.2.0"
        """
        self.version_string = version_string
        self.parts = [int(part) for part in version_string.split('.')]

    @classmethod
    def of(cls, version_string):
        """
        Factory method to create a Version instance.

        Args:
            version_string (str): Version string like "7.3.2.0"

        Returns:
            Version: New Version instance
        """
        return cls(version_string)

    @classmethod
    def current_cdh_parcel_version(cls):
        """
        Get the current CDH parcel version from environment or configuration.

        Returns:
            Version: Current version instance
        """
        # Option 1: Read from environment variable
        version_str = os.getenv('CDH_VERSION', '7.0.0.0')

        # Option 2: Read from a config file
        # config_path = os.path.join(os.path.dirname(__file__), 'version.conf')
        # if os.path.exists(config_path):
        #     with open(config_path, 'r') as f:
        #         version_str = f.read().strip()

        return cls(version_str)

    def __ge__(self, other):
        """Greater than or equal comparison."""
        for i in range(max(len(self.parts), len(other.parts))):
            self_part = self.parts[i] if i < len(self.parts) else 0
            other_part = other.parts[i] if i < len(other.parts) else 0

            if self_part > other_part:
                return True
            elif self_part < other_part:
                return False
        return True

    def __gt__(self, other):
        """Greater than comparison."""
        return not self.__le__(other)

    def __le__(self, other):
        """Less than or equal comparison."""
        return self == other or self < other

    def __lt__(self, other):
        """Less than comparison."""
        for i in range(max(len(self.parts), len(other.parts))):
            self_part = self.parts[i] if i < len(self.parts) else 0
            other_part = other.parts[i] if i < len(other.parts) else 0

            if self_part < other_part:
                return True
            elif self_part > other_part:
                return False
        return False

    def __eq__(self, other):
        """Equality comparison."""
        max_len = max(len(self.parts), len(other.parts))
        for i in range(max_len):
            self_part = self.parts[i] if i < len(self.parts) else 0
            other_part = other.parts[i] if i < len(other.parts) else 0
            if self_part != other_part:
                return False
        return True

    def __str__(self):
        return self.version_string

