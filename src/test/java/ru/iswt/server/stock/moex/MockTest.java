package ru.iswt.server.stock.moex;

import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import ru.iswt.testing.ExternalService;

/**
 * Created by admin on 22.12.2016.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({ExternalService.class})
public class MockTest {
    private final ExternalService externalService = PowerMockito.mock(ExternalService.class);

}
