import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { PriceChangeListener } from './price-change.listener.js';
import { PriceChangesGateway } from './price-changes.gateway.js';

@Module({
  imports: [DatabaseModule],
  providers: [PriceChangeListener, PriceChangesGateway],
  exports: [PriceChangeListener],
})
export class PriceChangesModule {}
