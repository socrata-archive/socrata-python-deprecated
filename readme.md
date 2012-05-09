The Socrata Python API
======================

This library provides an interface to the [SODA][] Publisher API. If you're new to all this, you may want to brush up on the [getting started][] guide.

If you're curious about how things work under the hood, you can also browse the [API documentation][] directly.

[soda]: http://dev.socrata.com/
[getting started]: http://dev.socrata.com/publisher/getting-started
[api documentation]: http://opendata.socrata.com/api/docs/


Installation
===========

Using PIP
---------

You can use [PIP][] to install from [git][]:

    pip install git+https://github.com/socrata/socrata-python.git

[pip]: http://www.pip-installer.org/en/latest/index.html
[git]: http://www.pip-installer.org/en/latest/usage.html#version-control-systems


Configuration
=============

In order to use the [SODA][] API, you'll need both a Socrata account and an application token. To create an account, visit the [Sign Up][] link on your preferred Socrata-powered data site.

Once you have an account, you can register an application by going to your [profile page][]. If you're not writing a web application, you can fill in the Callback Prefix with any https://server that's a valid URL as this will not be used. You can always come back and change these fields later.

[soda]: http://dev.socrata.com/
[sign up]: http://opendata.socrata.com/signup
[profile page]: http://opendata.socrata.com/profile/app_tokens

...more to be added.


Issues / Patches / Pull Requests...
===================================

... are welcome! If you add a new feature, please add tests so we don't accidentally break it in future releases.
