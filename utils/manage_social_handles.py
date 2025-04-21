from components.auth import get_local_storage

localS = get_local_storage()

def add_social_handle(platform, username):
    """Add a social handle to the list of handles in local storage."""
    if platform not in localS.getItem("social_handles"):
        localS.setItem("social_handles", {})
    
    handles = localS.getItem("social_handles")
    if platform not in handles:
        handles[platform] = []
    
    if username not in handles[platform]:
        handles[platform].append(username)
        localS.setItem("social_handles", handles)
        return True
    return False

def remove_social_handle(platform, username):
    """Remove a social handle from the list of handles in local storage."""
    handles = localS.getItem("social_handles")
    if platform in handles and username in handles[platform]:
        handles[platform].remove(username)
        localS.setItem("social_handles", handles)
        return True
    return False

def get_social_handles(platform):
    """Get the list of social handles for a specific platform."""
    localS = get_local_storage()
    handles = localS.getItem("social_handles")
    if platform in handles:
        return handles[platform]
    return []