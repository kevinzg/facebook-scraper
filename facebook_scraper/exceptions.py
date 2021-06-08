class NotFound(Exception):
    '''Post, page or profile not found / doesn't exist / deleted'''

    pass


class TemporarilyBanned(Exception):
    '''User account rate limited'''

    pass


class AccountDisabled(Exception):
    '''User account disabled, with option to appeal'''

    pass


class InvalidCookies(Exception):
    '''Cookies file passed but missing cookies'''

    pass


class LoginRequired(Exception):
    '''Facebook requires a login to see this'''

    pass


class LoginError(Exception):
    '''Failed to log in'''

    pass


class UnexpectedResponse(Exception):
    '''Facebook served something weird'''

    pass
