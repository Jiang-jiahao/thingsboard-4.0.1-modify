package org.thingsboard.server.actors;

/**
 * Actor创建器（实际是一个抽象工厂）
 * 运用了抽象工厂模式，具体工厂由子类实现
 */
public interface TbActorCreator {

    TbActorId createActorId();

    TbActor createActor();

}
