try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.iguser import IGUser
    from facebook_business.adobjects.igmedia import IGMedia

    print("Imports successful")
except ImportError as e:
    print(f"Import error: {e}")


