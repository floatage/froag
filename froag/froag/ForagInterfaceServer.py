#encoding=utf8

import socketserver, json, threading
import ForagInterfaceService

class ForagInterfaceHandler(socketserver.StreamRequestHandler):
    def handle(self):
        print('client ip:%s port:%s' % self.client_address)
        
        response = {}
        
        data = self.rfile.readline().decode()
        print('recv:%s' % data)
        requestParams = json.loads(data)
        ForagInterfaceService.serviceManager.getServiceObj(requestParams['name']).service(requestParams, response)
        print(len(response['result']), response['result'])
#         try:
#             data = self.rfile.readline().decode()
#             print('recv:%s' % data)
#             requestParams = json.loads(data)
#             print(requestParams)
#             self.serviceDict[requestParams['name']].service(requestParams, response)
#         except Exception:
#             response['state'] = 'failed'
#             response['reason'] = traceback.format_exc()
         
        self.wfile.write(json.dumps(response, ensure_ascii=False).encode())
   
class ForagInterfaceServer:
    def __init__(self, serve_addr, maxConnCnt=2):
        self.maxConnCnt = maxConnCnt
        self.server = socketserver.TCPServer(serve_addr, ForagInterfaceHandler)
        
    def run(self):
        for i in range(0, self.maxConnCnt):
            t = threading.Thread(target=self.server.serve_forever)
            t.setDaemon(True)
            t.start()
            
        self.server.serve_forever()

if __name__ == '__main__':
    server = ForagInterfaceServer(('', 9999))
    server.run()

#     conn = sqlite3.connect(FILE_DICT_DBNAME)
#     try:
#         cursor = conn.cursor()
#         pageIds = cursor.execute(GetHotArticleService.SQL_GET_POPULAREST_MSG_ID, (10,)).fetchall()[0]
#         pageIds = str(pageIds) if len(pageIds) > 1 else '(%d)' % pageIds[0]
#     finally:
#         conn.close()
#     hotMsg = []
#     conn = cx_Oracle.connect(DB_CONNECT_STRING_ORACLE)
#     try:
#         cursor = conn.cursor()
#         print(GetHotArticleService.SQL_GET_POPULAREST_MSG + pageIds)
#         result = cursor.execute(GetHotArticleService.SQL_GET_POPULAREST_MSG + pageIds, ())
#         colName = list(map(lambda x:x[0], cursor.description))
#         for row in result.fetchall():
#             colDict = {}
#             for index, col in enumerate(row):
#                 colDict[colName[index]] = col
#             hotMsg.append(colDict)
#     finally:
#         conn.close()
#     print(hotMsg)