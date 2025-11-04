# Patch Scrapy's downloader to fix fingerprinting bug
import scrapy.core.downloader

# Store original _get_slot method
_original_get_slot = scrapy.core.downloader.Downloader._get_slot


def _patched_get_slot(self, request, spider):
    """Patched version that handles list keys properly"""
    try:
        return _original_get_slot(self, request, spider)
    except TypeError as e:
        if "unhashable type: 'list'" in str(e):
            # Convert list to string for hashing
            key = str(request.url)
            if key not in self.slots:
                from scrapy.core.downloader import Slot

                self.slots[key] = Slot(
                    concurrency=self.total_concurrency,
                    delay=0,
                    randomize_delay=bool(getattr(self, "randomize_delay", True)),
                )
            return key, self.slots[key]
        else:
            raise


# Apply the patch
scrapy.core.downloader.Downloader._get_slot = _patched_get_slot
