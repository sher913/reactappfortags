import { NavLink } from "react-router-dom";
import React from "react";
import { useAuth0 } from "@auth0/auth0-react";

function IsAuthenticated() {
  const { isAuthenticated } = useAuth0();

  return isAuthenticated;
}

const MainNav = () => (
  <div className="navbar-nav me-auto">
    <NavLink to="/" exact className="nav-link" activeClassName="router-link-exact-active">
      Home
    </NavLink>

    {IsAuthenticated() ? (
      <NavLink to="/profile" exact className="nav-link" activeClassName="router-link-exact-active">
        Profile
      </NavLink>
    ) : null}
  </div>
);

export default MainNav;
