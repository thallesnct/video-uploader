import { RouterProvider } from "@tanstack/react-router";
import { router } from "./router";
import { SessionProvider } from "./session";

export function App() {
  return (
    <SessionProvider>
      <RouterProvider router={router} />
    </SessionProvider>
  );
}
