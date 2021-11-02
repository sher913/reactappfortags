import React from "react";
import { Route, Switch } from "react-router-dom";
import { useAuth0 } from "@auth0/auth0-react";

import { NavBar, Footer, Loading } from "./";
import { Home, Profile, ExternalApi } from "../views";

const LoadingIndicator = () => {
  const { isLoading } = useAuth0();

  if (isLoading) {
    return <Loading />;
  }

  return (
    <div id="app" className="d-flex flex-column h-100">
      <NavBar />
      <div className="container flex-grow-1">
        <Switch>
          <Route path="/profile" component={Profile} />
        </Switch>
      </div>
    </div>
  );
};

export { LoadingIndicator };
