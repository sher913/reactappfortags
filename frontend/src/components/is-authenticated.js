import React from "react";
import { useAuth0 } from "@auth0/auth0-react";

function IsAuthenticated() {
  const { isAuthenticated } = useAuth0();
  console.log(isAuthenticated);
  return isAuthenticated;
}

export default IsAuthenticated;
