import { Outlet, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { LoginGate } from "./pages/LoginGate/LoginGate";
import { UploadPage } from "./pages/UploadPage/UploadPage";
import { VideoDetailPage } from "./pages/VideoDetailPage/VideoDetailPage";
import { useSession } from "./session";

/** Every route renders under this. Not logged in → the login form, full
 * stop; the child routes below assume a session exists (useRequiredSession)
 * so the gate has to live above the router outlet, not inside a page. */
function SessionGate() {
  const { session } = useSession();
  return session ? <Outlet /> : <LoginGate />;
}

const rootRoute = createRootRoute({ component: SessionGate });

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: UploadPage,
});

const videoDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/videos/$videoId",
  component: VideoDetailPage,
});

const routeTree = rootRoute.addChildren([indexRoute, videoDetailRoute]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
