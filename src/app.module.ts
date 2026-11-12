import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AppController } from './app.controller.js';
import { AppService } from './app.service.js';
import { DatabaseModule } from './database/database.module.js';
import { PriceChangesModule } from './price-changes/price-changes.module.js';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    DatabaseModule,
    PriceChangesModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
