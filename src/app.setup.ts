import { INestApplication } from '@nestjs/common';
import { WsAdapter } from '@nestjs/platform-ws';

/** Configuração da aplicação, compartilhada entre `main.ts` e os testes e2e. */
export function setupApp(app: INestApplication) {
  app.enableShutdownHooks();
  // Um cliente que não lê o que recebe é desconectado ao acumular 1 MB.
  app.useWebSocketAdapter(new WsAdapter(app, { maxSendBufferSize: 1_048_576 }));
}
