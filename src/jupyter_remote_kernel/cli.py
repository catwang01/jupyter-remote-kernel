import argparse
import asyncio


def main() -> None:
    p = argparse.ArgumentParser(
        prog="jupyter-remote-kernel",
        description="Run remote Jupyter kernels through a reverse WebSocket tunnel.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # -- hub ------------------------------------------------------------------
    hp = sub.add_parser("hub", help="Start the Hub (run on a publicly reachable host)")
    hp.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    hp.add_argument("--port", type=int, default=8765, help="Port (default: 8765)")
    hp.add_argument("--token", default="", metavar="TOKEN",
                    help="Auth token (if set, agents and JupyterLab must supply it)")

    # -- agent ----------------------------------------------------------------
    ap = sub.add_parser(
        "agent",
        help="Register this machine as a remote kernel host (run on the remote machine)",
    )
    ap.add_argument("--hub", required=True, metavar="URL",
                    help="Hub URL, e.g. http://hub-ip:8765")
    ap.add_argument("--name", required=True, metavar="NAME",
                    help="Display name for this machine (used as kernel prefix)")
    ap.add_argument("--jupyter-port", type=int, default=0, metavar="PORT",
                    help="Local Jupyter Server port (default: auto)")
    ap.add_argument("--root-dir", default="", metavar="DIR",
                    help="Jupyter Server root directory (created if absent)")
    ap.add_argument("--token", default="", metavar="TOKEN",
                    help="Hub auth token (required if Hub was started with --token)")
    ap.add_argument("--debug", action="store_true",
                    help="Print all WebSocket messages to stdout")
    ap.add_argument("--extra-header", action="append", default=[], metavar="KEY:VALUE",
                    help="Extra HTTP header added to all Hub requests (repeatable)")

    args = p.parse_args()

    if args.cmd == "hub":
        from .hub import Hub
        Hub(token=args.token or None).run(host=args.host, port=args.port)

    elif args.cmd == "agent":
        from .agent import RemoteAgent
        extra_headers = {}
        for h in args.extra_header:
            if ":" not in h:
                p.error(f"--extra-header must be KEY:VALUE, got: {h!r}")
            k, _, v = h.partition(":")
            extra_headers[k.strip()] = v.strip()
        asyncio.run(RemoteAgent(
            args.hub, args.name, args.jupyter_port, args.root_dir,
            hub_token=args.token,
            debug=args.debug,
            extra_headers=extra_headers,
        ).run())


if __name__ == "__main__":
    main()
