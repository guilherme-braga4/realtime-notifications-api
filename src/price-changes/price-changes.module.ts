import { Module } from '@nestjs/common';
import { PriceChangeListener } from './price-change.listener.js';

@Module({
  providers: [PriceChangeListener],
  exports: [PriceChangeListener],
})
export class PriceChangesModule {}
