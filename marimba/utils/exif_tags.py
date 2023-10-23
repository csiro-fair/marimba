# Import the TAGS dictionary from the PIL.ExifTags module.
# This dictionary maps numeric EXIF tag values to their names.
from PIL.ExifTags import TAGS

# Define a dictionary of custom EXIF tags.
# The keys are the numeric tag values, and the values are the tag names.
marine_tags = {
    0xD000: "image-datetime",  # Date and time of the image capture
    0xD001: "image-latitude",  # Latitude where the image was captured
    0xD002: "image-longitude",  # Longitude where the image was captured
    0xD003: "image-depth",  # Depth below sea level where the image was captured
    0xD004: "image-altitude",  # TODO: Do we need both image-depth and image-altitude? (might be because they are defined by iFDO)
    0xD005: "image-sea-water-temperature",  # Sea water temperature at the time of capture
    0xD006: "image-sea-water-salinity",  # Sea water salinity at the time of capture
    0xD007: "image-sea-water-oxygen",  # Sea water oxygen levels at the time of capture
}

# Update the built-in TAGS dictionary with the custom tags.
# This will allow us to use our custom tags in the same way as the built-in tags.
TAGS.update(marine_tags)


def get_key(name):
    # Iterate over all items in the dictionary
    for key, val in TAGS.items():
        # If the current value matches the provided value
        if val == name:
            # Return the current key
            return key
    # If no key was found for the provided value, return None
    return None
