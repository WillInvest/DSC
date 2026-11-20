"""Read-only aggregate status endpoint; no files or control routes exposed."""
import argparse,asyncio,json
from pathlib import Path
import tornado.web

async def main():
    p=argparse.ArgumentParser();p.add_argument('--file',required=True);p.add_argument('--bind',required=True);p.add_argument('--port',type=int,default=18879);a=p.parse_args()
    class Status(tornado.web.RequestHandler):
        def get(self):
            try:value=json.loads(Path(a.file).read_text())
            except (OSError,ValueError):raise tornado.web.HTTPError(503,'HF status not yet published')
            self.set_header('Cache-Control','no-store');self.write(value)
    app=tornado.web.Application([(r'/status',Status)])
    app.listen(a.port,address=a.bind,max_body_size=4096);await asyncio.Event().wait()

if __name__=='__main__':asyncio.run(main())
