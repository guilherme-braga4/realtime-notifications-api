import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { EventsGateway } from './events.gateway.js';
import { EventsListener } from './events.listener.js';

@Module({
  imports: [DatabaseModule],
  providers: [EventsListener, EventsGateway],
  exports: [EventsListener],
})
export class EventsModule {}
