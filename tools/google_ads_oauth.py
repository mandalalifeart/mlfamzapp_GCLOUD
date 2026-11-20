"""One-time Google Ads OAuth (2026-11-20): loopback flow on localhost:8765, writes GOOGLE_ADS_REFRESH_TOKEN to .env."""
import http.server, os, urllib.parse, urllib.request, json, secrets, sys
CID=os.environ["GOOGLE_ADS_CLIENT_ID"]; SEC=os.environ["GOOGLE_ADS_CLIENT_SECRET"]
PORT=8765; RED=f"http://localhost:{PORT}/"; STATE=secrets.token_urlsafe(12)
url="https://accounts.google.com/o/oauth2/v2/auth?"+urllib.parse.urlencode({"client_id":CID,"redirect_uri":RED,"response_type":"code","scope":"https://www.googleapis.com/auth/adwords","access_type":"offline","prompt":"consent","state":STATE})
open("logs/google_ads_auth_url.txt","w").write(url); print(url, flush=True)
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if q.get("state",[""])[0]!=STATE or "code" not in q:
            self.send_response(400); self.end_headers(); self.wfile.write(b"bad request"); return
        body=urllib.parse.urlencode({"code":q["code"][0],"client_id":CID,"client_secret":SEC,"redirect_uri":RED,"grant_type":"authorization_code"}).encode()
        tok=json.load(urllib.request.urlopen("https://oauth2.googleapis.com/token",data=body))
        rt=tok.get("refresh_token")
        if rt:
            env=open(".env").read()
            if "GOOGLE_ADS_REFRESH_TOKEN=" not in env: open(".env","a").write(f"GOOGLE_ADS_REFRESH_TOKEN={rt}\n")
        self.send_response(200); self.end_headers(); self.wfile.write(b"Done - Google Ads connected to bababot. You can close this tab.")
        print("OK refresh token saved" if rt else f"NO refresh token {list(tok)}", flush=True)
        os._exit(0)
    def log_message(self,*a): pass
http.server.HTTPServer(("0.0.0.0",PORT),H).serve_forever()
