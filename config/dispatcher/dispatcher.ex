defmodule Dispatcher do
  use Matcher
  define_accept_types [
    html: [ "text/html", "application/xhtml+html" ],
    json: [ "application/json", "application/vnd.api+json" ],
    any: [ "*/*" ]
  ]

  @json %{ accept: %{ json: true } }
  @html %{ accept: %{ html: true } }
  @any %{ accept: %{ any: true } }

  # In order to forward the 'themes' resource to the
  # resource service, use the following forward rule:
  #
  # match "/themes/*path", @json do
  #   Proxy.forward conn, path, "http://resource/themes/"
  # end
  #
  # Run `docker-compose restart dispatcher` after updating
  # this file.

  post "/vocab-download-jobs/:id/run", @any do
    forward conn, [], "http://vocab-fetch/" <> id
  end

  post "/content-unification-jobs/:id/run", @any do
    forward conn, [], "http://content-unification/" <> id
  end

  match "/vocabularies/*path", @any do
    forward conn, path, "http://resource/vocabularies/"
  end

  match "/vocab-download-jobs/*path", @any do
    forward conn, path, "http://resource/vocab-download-jobs/"
  end

  match "/content-unification-jobs/*path", @any do
    forward conn, path, "http://resource/content-unification-jobs/"
  end

  match "/concepts/search", @any do
    forward conn, [], "http://search/concepts/search"
  end

  match "/assets/*path", @any do
    forward conn, path, "http://frontend/assets/"
  end

  match "/*_path", @html do
    # *_path allows a path to be supplied, but will not yield
    # an error that we don't use the path variable.
    forward conn, [], "http://frontend/index.html"
  end

  match "/*_", %{ last_call: true, accept: %{ json: true } } do
    send_resp( conn, 404, "{ \"error\": { \"code\": 404, \"message\": \"Route not found.  See config/dispatcher.ex\" } }" )
  end

  match "/*_", %{ last_call: true } do
    send_resp( conn, 404, "Route not found.  See config/dispatcher.ex" )
  end
end
