import React from "react";
import { useAuth0 } from "@auth0/auth0-react";
const UpdateButton = () => {
  const { isAuthenticated } = useAuth0();
  return isAuthenticated ? (
    <div className="navbar-nav me-auto">
      <button id="test" className="btn btn-primary btn-block">
        Update Datahub GMS
      </button>
    </div>
  ) : (
    <div className="navbar-nav me-auto">
      <button id="test" className="btn btn-primary btn-block" style={{ display: "none" }}>
        Update Datahub GMS
      </button>
    </div>
  );
};

export default UpdateButton;
