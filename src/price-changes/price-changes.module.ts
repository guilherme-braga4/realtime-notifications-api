import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { PriceChangeListener } from './price-change.listener.js';

@Module({
  imports: [DatabaseModule],
  providers: [PriceChangeListener],
  exports: [PriceChangeListener],
})
export class PriceChangesModule {}
