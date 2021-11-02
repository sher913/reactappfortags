import React from "react";

import MainNav from "./main-nav";
import AuthNav from "./auth-nav";
import UpdateButton from "./update-button";
const NavBar = () => {
  return (
    <div className="nav-container mb-3">
      <nav className="navbar navbar-expand-md navbar-light bg-light">
        <div className="navbar-brand logo" />
        <UpdateButton />
        <MainNav />
        <AuthNav />
      </nav>
    </div>
  );
};

export default NavBar;
